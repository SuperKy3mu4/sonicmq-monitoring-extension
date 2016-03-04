package com.appdynamics.extensions.sonicmq;


import com.appdynamics.extensions.PathResolver;
import com.appdynamics.extensions.file.FileLoader;
import com.appdynamics.extensions.sonicmq.config.ComponentConfig;
import com.appdynamics.extensions.sonicmq.config.Configuration;
import com.appdynamics.extensions.yml.YmlReader;
import com.google.common.collect.Maps;
import com.singularity.ee.agent.systemagent.api.AManagedMonitor;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;
import com.sonicsw.ma.mgmtapi.config.MgmtException;
import com.sonicsw.mf.common.runtime.IComponentState;
import com.sonicsw.mf.common.runtime.IContainerState;
import com.sonicsw.mf.common.runtime.IState;
import com.sonicsw.mf.jmx.client.JMSConnectorClient;
import com.sonicsw.mf.mgmtapi.config.IContainerBean;
import com.sonicsw.mf.mgmtapi.runtime.IAgentManagerProxy;
import com.sonicsw.mq.mgmtapi.config.MQMgmtBeanFactory;
import com.sonicsw.mx.config.ConfigServiceException;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * This extension will extract metrics from Sonic MQ.
 */

public class SonicMqMonitor extends AManagedMonitor{

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SonicMqMonitor.class);
    public static final String CONFIG_ARG = "config-file";
    public static final String BROKER = "MQ_BROKER";
    public static final String CONTAINER = "MF_CONTAINER";
    private volatile boolean initialized;
    private Configuration config;

    public SonicMqMonitor() {
        System.out.println(logVersion());
    }

    public TaskOutput execute(Map<String, String> taskArgs, TaskExecutionContext taskExecutionContext) throws TaskExecutionException {
        try {
            initialize(taskArgs);
            JMSConnectorClient client = new JMSConnectorClient();
            try {
                //connect JMX
                JMSConnectionFactory.connect(client, config.getLocation(), config.getUsername(), config.getPassword(), config.getTimeout());
                final BrokerCollector brokerCollector = new BrokerCollector(client,this);
                brokerCollector.collectAndReport(config);
                final ContainerCollector containerCollector = new ContainerCollector(client,this);
                containerCollector.collectAndReport(config);
            }
            finally {
                JMSConnectionFactory.disconnect(client, config.getLocation());
            }
            logger.info("SonicMQ monitor run completed successfully.");
            return new TaskOutput("SonicMQ monitor run completed successfully.");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Metrics collection failed", e);
        }
        throw new TaskExecutionException("SonicMQ monitoring run completed with failures.");

    }


    private void initialize(Map<String, String> taskArgs) {
        if(!initialized){
            //read the config.
            final String configFilePath = taskArgs.get(CONFIG_ARG);
            File configFile = PathResolver.getFile(configFilePath, AManagedMonitor.class);
            if(configFile != null && configFile.exists()){
                FileLoader.load(new FileLoader.Listener() {
                    public void load(File file) {
                        String path = file.getAbsolutePath();
                        try {
                            if (path.contains(configFilePath)) {
                                logger.info("The file [{}] has changed, reloading the config", file.getAbsolutePath());
                                reloadConfig(file);
                            } else {
                                logger.warn("Unknown file [{}] changed, ignoring", file.getAbsolutePath());
                            }
                        } catch (Exception e) {
                            logger.error("Exception while reloading the file " + file.getAbsolutePath(), e);
                        }
                    }
                }, configFilePath);
            }
            else{
                logger.error("Config file is not found.The config file path {} is resolved to {}",
                        taskArgs.get(CONFIG_ARG), configFile != null ? configFile.getAbsolutePath() : null);
            }

            initialized = true;
        }
    }

    private void reloadConfig(File file) throws MgmtException, ConfigServiceException {
        config = YmlReader.readFromFile(file, Configuration.class);
        if (config != null) {
            loadCollectiveState();
            logger.info("The config file was reloaded successfully.");
        }
        else {
            throw new IllegalArgumentException("The config cannot be initialized from the file " + file.getAbsolutePath());
        }
    }

    private Map<String,String> buildTierMap() throws MgmtException, ConfigServiceException {
        Map<String,String> tierMap = Maps.newHashMap();
        MQMgmtBeanFactory factory = new MQMgmtBeanFactory();
        try{
            ManagementBeanFactory.connect(factory,config.getLocation(),config.getDomain(),config.getUsername(),config.getPassword());
            List<String> containerBeanNames = factory.getContainerBeanNames();
            logger.debug("*********Container Bean Info # ********");
            for (String containerBean : containerBeanNames) {
                IContainerBean container = factory.getContainerBean(containerBean);
                String jvmArgs = container.getJvmArguments();
                logger.debug("Container name {}",container.getContainerName());
                logger.debug("JVM arguments are {}",jvmArgs);
                String tierName = Util.getTierName(jvmArgs);
                if(tierName != null){
                    tierMap.put(container.getContainerName(), tierName);
                }
            }
        }
        finally{
            ManagementBeanFactory.disconnect(factory);
        }
        return tierMap;
    }



    private void loadCollectiveState() throws MgmtException, ConfigServiceException {
        Map<String,String> tierMap = buildTierMap();
        config.setTierMap(tierMap);
        JMSConnectorClient client = new JMSConnectorClient();
        try{
            //connect JMX
            JMSConnectionFactory.connect(client, config.getLocation(), config.getUsername(), config.getPassword(), config.getTimeout());
            //get default domain
            long startTime = System.currentTimeMillis();
            String domain = config.getDomain();
            logger.debug("The domain is {}", domain);
            String hostname = Util.getHostname();
            logger.debug("The hostname for this machine is {}",hostname);
            String jmxName = domain + "." + IAgentManagerProxy.GLOBAL_ID + ":ID=" + IAgentManagerProxy.GLOBAL_ID;
            IAgentManagerProxy agentManagerProxy = ProxyUtil.getAgentManagerProxy(client, jmxName);
            IState[] containerStates = agentManagerProxy.getCollectiveState();
            int i=1;
            for (IState aContainerState : containerStates) {
                IContainerState containerState = (IContainerState) aContainerState;
                logger.debug("*********Container Info # {} ********", i);
                String containerJmxName = containerState.getRuntimeIdentity().getCanonicalName();
                logger.debug("Container Canonical Name = {}", containerJmxName);
                String containerDisplayName = containerState.getRuntimeIdentity().getContainerName();
                logger.debug("Container Name = {}", containerDisplayName);
                logger.debug("Container Domain Name = {}", containerState.getRuntimeIdentity().getDomainName());
                String containerHost = containerState.getContainerHost();
                containerHost = getCanonicalHostName(containerHost);
                logger.debug("Container Host = {}", containerHost);
                IState[] componentStates = containerState.getComponentStates();
                if(containerHost.equalsIgnoreCase(hostname)) {
                    int j=1;
                    for (IState aComponentState : componentStates) {
                        IComponentState componentState = (IComponentState) aComponentState;
                        logger.debug("\t*********Component Info # {} ********", j);
                        String componentJmxName = componentState.getRuntimeIdentity().getCanonicalName();
                        logger.debug("\tComponent Canonical Name = {}", componentJmxName);
                        String componentContainerName = componentState.getRuntimeIdentity().getContainerName();
                        logger.debug("\tComponent Container Name = {}", componentContainerName);
                        logger.debug("\tComponent Domain Name = {}", componentState.getRuntimeIdentity().getDomainName());
                        String brId = aComponentState.getRuntimeIdentity().toString();
                        String componentDisplayName = brId.substring(brId.indexOf("=") + 1);
                        logger.debug("\tBroker Name = {}", componentDisplayName);
                        String type = "";
                        if(componentState.getRuntimeIdentity() != null &&
                                componentState.getRuntimeIdentity().getConfigIdentity() != null &&
                                componentState.getRuntimeIdentity().getConfigIdentity().getType() != null){
                            logger.info("\tComponent Type = {}", componentState.getRuntimeIdentity().getConfigIdentity().getType());
                            type = componentState.getRuntimeIdentity().getConfigIdentity().getType();
                        }
                        if(type.equalsIgnoreCase(BROKER)) {
                            ComponentConfig brokerConfig = new ComponentConfig(componentDisplayName, componentJmxName,componentContainerName);
                            config.getBrokerConfigs().add(brokerConfig);
                        }
                        else if(type.equalsIgnoreCase(CONTAINER)){
                            String containerJMXName = domain + "." + containerDisplayName + ":ID=" + IAgentManagerProxy.GLOBAL_ID;
                            ComponentConfig containerConfig = new ComponentConfig(containerDisplayName,containerJMXName,containerDisplayName);
                            config.getContainerConfigs().add(containerConfig);
                        }
                        j++;
                    }
                }
                i++;
            }

            long endTime = System.currentTimeMillis();
            logger.debug("Total time taken to get list of all containers = {}", endTime - startTime);


        } catch (MalformedObjectNameException e) {
            logger.error("Something unknown happened",e);
        } finally{
            JMSConnectionFactory.disconnect(client, config.getLocation());
        }

    }

    private String logVersion() {
        String msg = "Using Monitor Version [" + getImplementationVersion() + "]";
        logger.info(msg);
        return msg;
    }

    public static String getImplementationVersion() {
        return SonicMqMonitor.class.getPackage().getImplementationTitle();
    }

    private String getCanonicalHostName(String containerHost) {
        if(containerHost.indexOf(".") != -1){
            return containerHost.substring(0,containerHost.indexOf("."));
        }
        return containerHost;
    }

}
