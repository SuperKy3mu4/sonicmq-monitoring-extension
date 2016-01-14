package com.appdynamics.extensions.sonicmq;


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
import com.sonicsw.mq.common.runtime.IDurableSubscriptionData;
import com.sonicsw.mq.mgmtapi.runtime.IBrokerProxy;
import com.sonicsw.mq.mgmtapi.runtime.MQProxyFactory;
import org.slf4j.LoggerFactory;
import progress.message.ft.ReplicationState;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static com.appdynamics.extensions.util.metrics.MetricConstants.METRICS_SEPARATOR;

public class BrokerCollector {

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
            IAgentManagerProxy agentManagerProxy = getAgentManagerProxy(client,domain);
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
                        getABrokerMetrics(client,config,brokerJmxName,brokerName,metrics);
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
        return metrics;
    }

    private String getCanonicalHostName(String containerHost) {
        if(containerHost.indexOf(".") != -1){
            return containerHost.substring(0,containerHost.indexOf("."));
        }
        return containerHost;
    }

    private void getABrokerMetrics(JMSConnectorClient client,Configuration config, String brokerJmxName,String brokerName,Map<String, String> metrics) {
        try {

            IBrokerProxy proxy = getBrokerProxy(client, brokerJmxName);

            metrics.put(brokerName + METRICS_SEPARATOR + "IsPrimary", getReplicationType(proxy));
            metrics.put(brokerName + METRICS_SEPARATOR + "IsActive",proxy.getState().toString());
            metrics.put(brokerName + METRICS_SEPARATOR + "ReplicationState", proxy.getReplicationState().toString());

            if(isReplicationStateActive(proxy)) {

                //set instance metrics
                setMetrics(proxy, brokerName,metrics, config.getQueueExcludePatterns());
                setTopicMetrics(proxy,brokerName,metrics,config.getUserExcludePatterns(),config.getTopicExcludePatterns());
            }

        }
        catch (MalformedObjectNameException e) {
            logger.error("Failed to create proxy for id '"+ brokerJmxName,e);
        }
        catch(Exception e){
            logger.error("Failed to fetch metrics for " + brokerJmxName,e);
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

    private boolean isReplicationStateActive(IBrokerProxy proxy) {
        return proxy.getReplicationState() == ReplicationState.ACTIVE;
    }

    private String getReplicationType(IBrokerProxy proxy) {
        return "PRIMARY".equals(proxy.getReplicationType()) ? IsPrimary : IsBackup;
    }

    private void setTopicMetrics(IBrokerProxy proxy, String brokerName, Map<String, String> metrics,List<String> userExcludePatterns,List<String> topicExcludePatterns) {
        //get topics for all users
        List<String> users = proxy.getUsersWithDurableSubscriptions(null);
        if(users != null){
            for(String user : users){
                if(!isExcluded(user, userExcludePatterns)){
                    List<IDurableSubscriptionData> topics = proxy.getDurableSubscriptions(user);
                    if(topics != null){
                        for(IDurableSubscriptionData topic : topics){
                            if(!isExcluded(topic.getTopicName(),topicExcludePatterns)){
                                String topicMetricPrefix = brokerName + METRICS_SEPARATOR + "users" + METRICS_SEPARATOR +
                                        user + METRICS_SEPARATOR + "topics" + METRICS_SEPARATOR + topic.getTopicName() + METRICS_SEPARATOR;
                                metrics.put(topicMetricPrefix + "MessageCount", MetricUtils.toWholeNumberString(topic.getMessageCount()));
                                metrics.put(topicMetricPrefix + "TotalMessageSize", MetricUtils.toWholeNumberString(topic.getMessageSize()));
                            }
                        }
                    }
                }
            }
        }
    }

    private void setMetrics(IBrokerProxy proxy, String brokerName, Map<String, String> metrics, List<String> excludePatterns) {
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
                metricName = brokerName + METRICS_SEPARATOR + metricName;
                metrics.put(metricName, MetricUtils.toWholeNumberString(m.getValue()));
            }
        }
        logger.debug("*******Metric Details End*********");
        logger.debug("Collected basic broker metrics.");
    }

    public String getMetricName(IMetricIdentity metricIdentity) {
        if(metricIdentity != null && metricIdentity.getNameComponents()!= null && metricIdentity.getNameComponents().length == 3){
            return metricIdentity.getNameComponents()[0] + METRICS_SEPARATOR + metricIdentity.getNameComponents()[1] + METRICS_SEPARATOR + metricIdentity.getNameComponents()[2];
        }
        logger.warn("Metric not found - " + metricIdentity.getName() + " ; Absolute Name = " + metricIdentity.getAbsoluteName());
        return "";
    }

    protected final IBrokerProxy getBrokerProxy(JMSConnectorClient client, String jmxName) throws MalformedObjectNameException {
        return MQProxyFactory.createBrokerProxy(client, new ObjectName(jmxName));
    }

    protected final IAgentManagerProxy getAgentManagerProxy(JMSConnectorClient client,String domain) throws MalformedObjectNameException {
        ObjectName jmxName = new ObjectName(domain + "." + IAgentManagerProxy.GLOBAL_ID + ":ID=" + IAgentManagerProxy.GLOBAL_ID);
        return MFProxyFactory.createAgentManagerProxy(client, jmxName);
    }


}
