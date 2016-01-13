package com.appdynamics.extensions.sonicmq;


import com.appdynamics.extensions.sonicmq.config.BrokerConfig;
import com.appdynamics.extensions.sonicmq.config.Configuration;
import com.appdynamics.extensions.util.MetricUtils;
import com.google.common.base.Strings;
import com.sonicsw.mf.common.metrics.IMetric;
import com.sonicsw.mf.common.metrics.IMetricIdentity;
import com.sonicsw.mf.common.runtime.IComponentState;
import com.sonicsw.mf.common.runtime.IContainerState;
import com.sonicsw.mf.common.runtime.IState;
import com.sonicsw.mf.jmx.client.JMSConnectorClient;
import com.sonicsw.mf.mgmtapi.runtime.IAgentManagerProxy;
import com.sonicsw.mf.mgmtapi.runtime.MFProxyFactory;
import com.sonicsw.mq.mgmtapi.runtime.IBrokerProxy;
import com.sonicsw.mq.mgmtapi.runtime.MQProxyFactory;
import org.slf4j.LoggerFactory;
import progress.message.ft.ReplicationState;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class BrokerCollector extends Collector{

    public static final String IsPrimary = "1";
    public static final String IsBackup = "0";
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(BrokerCollector.class);

    private static final IMetricIdentity[] metricIds = new IMetricIdentity[] {

            IBrokerProxy.BROKER_BYTES_DELIVEREDPERSECOND_METRIC_ID,
            IBrokerProxy.BROKER_BYTES_RECEIVEDPERSECOND_METRIC_ID,
            IBrokerProxy.BROKER_BYTES_TOPICDBSIZE_METRIC_ID,

            IBrokerProxy.BROKER_CONNECTIONS_COUNT_METRIC_ID,
            IBrokerProxy.BROKER_CONNECTIONS_REJECTEDPERMINUTE_METRIC_ID,

            IBrokerProxy.BROKER_MESSAGES_DELIVERED_METRIC_ID,
            IBrokerProxy.BROKER_MESSAGES_RECEIVED_METRIC_ID,
            IBrokerProxy.BROKER_MESSAGES_DELIVEREDPERSECOND_METRIC_ID,
            IBrokerProxy.BROKER_MESSAGES_RECEIVEDPERSECOND_METRIC_ID,

            IBrokerProxy.CONNECTION_BYTES_DELIVERED_METRIC_ID,
            IBrokerProxy.CONNECTION_BYTES_DELIVEREDPERSECOND_METRIC_ID,
            IBrokerProxy.CONNECTION_BYTES_RECEIVED_METRIC_ID,
            IBrokerProxy.CONNECTION_BYTES_RECEIVEDPERSECOND_METRIC_ID,

            IBrokerProxy.CONNECTION_MESSAGES_DELIVERED_METRIC_ID,
            IBrokerProxy.CONNECTION_MESSAGES_DELIVEREDPERSECOND_METRIC_ID,
            IBrokerProxy.CONNECTION_MESSAGES_RECEIVED_METRIC_ID,
            IBrokerProxy.CONNECTION_MESSAGES_RECEIVEDPERSECOND_METRIC_ID,

            IBrokerProxy.QUEUE_MESSAGES_COUNT_METRIC_ID,
            IBrokerProxy.QUEUE_MESSAGES_DELIVEREDPERSECOND_METRIC_ID,
            IBrokerProxy.QUEUE_MESSAGES_MAXAGE_METRIC_ID,
            IBrokerProxy.QUEUE_MESSAGES_MAXDEPTH_METRIC_ID,
            IBrokerProxy.QUEUE_MESSAGES_RECEIVEDPERSECOND_METRIC_ID,
            IBrokerProxy.QUEUE_MESSAGES_SIZE_METRIC_ID,
            IBrokerProxy.QUEUE_MESSAGES_TIMEINQUEUE_METRIC_ID
    };




    public Map<String, String> getMetrics(Configuration config) {
        logger.debug("Getting metrics from Broker. ");
        Map<String,String> metrics = new HashMap<String, String>();
        try{
            //connect JMX
            connect(config.getLocation(),config.getUsername(),config.getPassword(),config.getTimeout());
            //get default domain
            long startTime = System.currentTimeMillis();

            String domain = "dmWdwMmp_Load2";
            logger.info("The domain is {}", domain);
            String hostname = null;
            hostname = getHostname();
            logger.info("The hostname for this machine is {}",hostname);
            IAgentManagerProxy agentManagerProxy = getAgentManagerProxy(client,domain);
            IState[] containerStates = agentManagerProxy.getCollectiveState();
            int i=1;
            for (IState aContainerState : containerStates) {
                IContainerState containerState = (IContainerState) aContainerState;
                logger.info("*********Container Info # {} ********",i);
                logger.info("Container Canonical Name = {}", containerState.getRuntimeIdentity().getCanonicalName());
                logger.info("Container Name = {}", containerState.getRuntimeIdentity().getContainerName());
                logger.info("Container Domain Name = {}", containerState.getRuntimeIdentity().getDomainName());
                IState[] componentStates = containerState.getComponentStates();
                int j=1;
                for(IState aComponentState : componentStates){
                    IComponentState componentState = (IComponentState)aComponentState;
                    logger.info("\t*********Component Info # {} ********",j);
                    logger.info("\tComponent Canonical Name = {}",componentState.getRuntimeIdentity().getCanonicalName());
                    logger.info("\tComponent Container Name = {}",componentState.getRuntimeIdentity().getContainerName());
                    logger.info("\tComponent Domain Name = {}",componentState.getRuntimeIdentity().getDomainName());
                    String brId=aComponentState.getRuntimeIdentity().toString();
                    String brokerName = brId.substring(brId.indexOf("=")+1);
                    logger.info("\tBroker Name = {}",brokerName);
                    j++;
                }
                i++;
            }

            long endTime = System.currentTimeMillis();
            logger.info("Total time taken to get list of all containers = {}", endTime - startTime);


        } catch (MalformedObjectNameException e) {
            logger.error("Something unknown happened",e);
        } finally{
            disconnect(config.getLocation());
        }
        return metrics;
    }

    private void getABrokerMetrics(Configuration config, Map<String, String> metrics,BrokerConfig aBrokerConfig) {
        try {

            IBrokerProxy proxy = getProxy(client, new ObjectName(aBrokerConfig.getObjectName()));

            metrics.put(aBrokerConfig.getDisplayName() + METRIC_SEPARATOR + "IsPrimary", getReplicationType(proxy));
            metrics.put(aBrokerConfig.getDisplayName() + METRIC_SEPARATOR + "IsActive",proxy.getState().toString());
            metrics.put(aBrokerConfig.getDisplayName() + METRIC_SEPARATOR + "ReplicationState", proxy.getReplicationState().toString());

            if(isReplicationStateActive(proxy)) {

                //set instance metrics
                setMetrics(proxy, aBrokerConfig, metrics, config.getQueueExcludePatterns());
            }

        }
        catch (MalformedObjectNameException e) {
            logger.error("Failed to create proxy for id '"+ aBrokerConfig.getObjectName() +"': "+e);
        }
        catch(Exception e){
            logger.error("Failed to fetch metrics for " + aBrokerConfig.getObjectName() + " : " + e);
        }
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


    private boolean isExcluded(String element, List<String> excludePatterns) {
        if(excludePatterns != null) {
            for (String pattern : excludePatterns) {
                if (element.matches(pattern)) {
                    return true;
                }
            }
        }
        return false;
    }

/*    private boolean isBrokerActive(IBrokerProxy proxy) {
        return proxy.getState() == IComponentState.STATE_ONLINE;
    }*/


    private boolean isReplicationStateActive(IBrokerProxy proxy) {
        return proxy.getReplicationState() == ReplicationState.ACTIVE;
    }

    private String getReplicationType(IBrokerProxy proxy) {
        return "PRIMARY".equals(proxy.getReplicationType()) ? IsPrimary : IsBackup;
    }

    private void setMetrics(IBrokerProxy proxy, BrokerConfig aBrokerConfig, Map<String, String> metrics, List<String> excludePatterns) {
        //IMetricInfo[] metricInfo = proxy.getMetricsInfo();
        /*logger.debug("*******Metric Details Start*********");
        for(IMetricInfo info : metricInfo){
            logger.debug("Metric Identity Name: " + info.getMetricIdentity().getName());
            logger.debug("Metric Identity Absolute Name: " + info.getMetricIdentity().getAbsoluteName());
            logger.debug("Metric Info Description: " + info.getDescription());
            logger.debug("Metric Info Value Type: " + info.getValueType());
            logger.debug("Metric Info Dynamic: " + info.isDynamic());
            logger.debug("Metric Info Instance Metric: " + info.isInstanceMetric());
        }*/
        long startTime = System.currentTimeMillis();
        IMetricIdentity[] activeMetrics = proxy.getActiveMetrics(metricIds);
        long endTime = System.currentTimeMillis();
        long duration1 = endTime - startTime;
        logger.debug("Time to execute getActiveMetrics call = {}",duration1);

        startTime = System.currentTimeMillis();
        IMetric[] data = proxy.getMetricsData(activeMetrics, false).getMetrics();
        endTime = System.currentTimeMillis();
        long duration2 = endTime - startTime;
        logger.debug("Time to execute getMetricsData call = {}",duration2);
        logger.debug("Total time taken = {}",duration1 + duration2);

        for (IMetric m : data) {
            String metricName = getMetricName(m.getMetricIdentity());
            if (!Strings.isNullOrEmpty(metricName) && !isExcluded(metricName,excludePatterns)) {
                logger.debug("Metric Name : {} ,Metric Value : {}",metricName,m.getValue());
                metricName = aBrokerConfig.getDisplayName() + Collector.METRIC_SEPARATOR + metricName;
                metrics.put(metricName, MetricUtils.toWholeNumberString(m.getValue()));
            }
        }
        logger.debug("*******Metric Details End*********");
        logger.debug("Collected basic broker metrics.");
    }



    protected final IBrokerProxy getProxy(JMSConnectorClient client, ObjectName jmxName) {
        return MQProxyFactory.createBrokerProxy(client, jmxName);
    }

    protected final IAgentManagerProxy getAgentManagerProxy(JMSConnectorClient client,String domain) throws MalformedObjectNameException {
        ObjectName jmxName = new ObjectName(domain + "." + IAgentManagerProxy.GLOBAL_ID + ":ID=" + IAgentManagerProxy.GLOBAL_ID);
        return MFProxyFactory.createAgentManagerProxy(client, jmxName);
    }


}
