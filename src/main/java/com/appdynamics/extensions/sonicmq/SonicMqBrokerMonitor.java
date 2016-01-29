package com.appdynamics.extensions.sonicmq;


import com.appdynamics.extensions.PathResolver;
import com.appdynamics.extensions.file.FileLoader;
import com.appdynamics.extensions.sonicmq.config.BrokerConfig;
import com.appdynamics.extensions.sonicmq.config.Configuration;
import com.appdynamics.extensions.yml.YmlReader;
import com.google.common.base.Strings;
import com.singularity.ee.agent.systemagent.api.AManagedMonitor;
import com.singularity.ee.agent.systemagent.api.MetricWriter;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;
import com.sonicsw.mf.common.runtime.IComponentState;
import com.sonicsw.mf.common.runtime.IContainerState;
import com.sonicsw.mf.common.runtime.IState;
import com.sonicsw.mf.jmx.client.JMSConnectorClient;
import com.sonicsw.mf.mgmtapi.runtime.IAgentManagerProxy;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * This extension will extract metrics from Sonic MQ.
 */

public class SonicMqBrokerMonitor extends AManagedMonitor{

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SonicMqBrokerMonitor.class);
    public static final String METRIC_SEPARATOR = "|";
    public static final String CONFIG_ARG = "config-file";
    public static final String TIER_NAME = "appdynamics.agent.tierName";
    private volatile boolean initialized;
    private Configuration config;

    public SonicMqBrokerMonitor() {
        System.out.println(logVersion());
    }

    public TaskOutput execute(Map<String, String> taskArgs, TaskExecutionContext taskExecutionContext) throws TaskExecutionException {
        try {
            initialize(taskArgs);
            final BrokerCollector brokerCollector = new BrokerCollector();
            Map<String,String> metrics = brokerCollector.getMetrics(config);
            printMetrics(config, metrics);
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

    private void setMetricPrefix() {
        String prefix = config.getMetricPrefix();
        if(!Strings.isNullOrEmpty(prefix)){
            String tierName = System.getProperty(TIER_NAME);
            if(!Strings.isNullOrEmpty(tierName)){
                prefix = prefix.replaceFirst("<TIER_NAME>",tierName);
                config.setMetricPrefix(prefix);
            }
        }
        else {
            config.setMetricPrefix("Custom Metrics|SonicMq|");
        }
    }

    private void reloadConfig(File file) {
        config = YmlReader.readFromFile(file, Configuration.class);
        if (config != null) {
            setMetricPrefix();
            loadCollectiveState();
            logger.info("The config file was reloaded successfully.");
        }
        else {
            throw new IllegalArgumentException("The config cannot be initialized from the file " + file.getAbsolutePath());
        }
    }

    private void loadCollectiveState() {
        JMSConnectorClient client = new JMSConnectorClient();
        try{
            //connect JMX
            ConnectionUtil.connect(client,config.getLocation(),config.getUsername(),config.getPassword(),config.getTimeout());
            //get default domain
            long startTime = System.currentTimeMillis();
            String domain = config.getDomain();
            logger.debug("The domain is {}", domain);
            String hostname = getHostname();
            logger.debug("The hostname for this machine is {}",hostname);
            IAgentManagerProxy agentManagerProxy = ProxyUtil.getAgentManagerProxy(client, domain);
            IState[] containerStates = agentManagerProxy.getCollectiveState();
            int i=1;
            for (IState aContainerState : containerStates) {
                IContainerState containerState = (IContainerState) aContainerState;
                logger.debug("*********Container Info # {} ********", i);
                logger.debug("Container Canonical Name = {}", containerState.getRuntimeIdentity().getCanonicalName());
                logger.debug("Container Name = {}", containerState.getRuntimeIdentity().getContainerName());
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
                        String brokerJmxName = componentState.getRuntimeIdentity().getCanonicalName();
                        logger.debug("\tComponent Canonical Name = {}", brokerJmxName);
                        logger.debug("\tComponent Container Name = {}", componentState.getRuntimeIdentity().getContainerName());
                        logger.debug("\tComponent Domain Name = {}", componentState.getRuntimeIdentity().getDomainName());
                        String brId = aComponentState.getRuntimeIdentity().toString();
                        String brokerName = brId.substring(brId.indexOf("=") + 1);
                        logger.debug("\tBroker Name = {}", brokerName);
                        BrokerConfig brokerConfig = new BrokerConfig(brokerName,brokerJmxName);
                        config.getBrokerConfigs().add(brokerConfig);
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
            ConnectionUtil.disconnect(client, config.getLocation());
        }

    }

    private void printMetrics(Configuration config,Map<String, String> metrics) {
        for(Map.Entry<String,String> entry : metrics.entrySet()){
            printAverageAverageIndividual(config.getMetricPrefix() + entry.getKey(), entry.getValue());
        }
    }


    private void printAverageAverageIndividual(String metricPath, String metricValue) {
        printMetric(metricPath, metricValue,
                MetricWriter.METRIC_AGGREGATION_TYPE_AVERAGE,
                MetricWriter.METRIC_TIME_ROLLUP_TYPE_AVERAGE,
                MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_INDIVIDUAL
        );
    }


    private String logVersion() {
        String msg = "Using Monitor Version [" + getImplementationVersion() + "]";
        logger.info(msg);
        return msg;
    }

    /**
     * A helper method to report the metrics.
     * @param metricPath
     * @param metricValue
     * @param aggType
     * @param timeRollupType
     * @param clusterRollupType
     */
    private void printMetric(String metricPath,String metricValue,String aggType,String timeRollupType,String clusterRollupType) {
        MetricWriter metricWriter = getMetricWriter(metricPath,
                aggType,
                timeRollupType,
                clusterRollupType
        );
        //System.out.println(getLogPrefix()+"Sending [" + aggType + METRIC_SEPARATOR + timeRollupType + METRIC_SEPARATOR + clusterRollupType
        //            + "] metric = " + metricPath + " = " + metricValue);
        if (logger.isDebugEnabled()) {
            logger.debug("Sending [" + aggType + METRIC_SEPARATOR + timeRollupType + METRIC_SEPARATOR + clusterRollupType
                    + "] metric = " + metricPath + " = " + metricValue);
        }
        metricWriter.printMetric(metricValue);
    }


    public static String getImplementationVersion() {
        return SonicMqBrokerMonitor.class.getPackage().getImplementationTitle();
    }

    private String getCanonicalHostName(String containerHost) {
        if(containerHost.indexOf(".") != -1){
            return containerHost.substring(0,containerHost.indexOf("."));
        }
        return containerHost;
    }

    private String getHostname() {
        String hostname;
        try {
            // See http://stackoverflow.com/questions/7348711/recommended-way-to-get-hostname-in-java
            // for information on when this will fail.
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException needToAskSystem) {
            hostname = null;
        }
        return hostname;
    }
}
