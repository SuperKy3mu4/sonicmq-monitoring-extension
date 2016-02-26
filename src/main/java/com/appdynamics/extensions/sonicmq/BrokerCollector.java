package com.appdynamics.extensions.sonicmq;


import com.appdynamics.extensions.sonicmq.config.ComponentConfig;
import com.appdynamics.extensions.sonicmq.config.Configuration;
import com.appdynamics.extensions.util.MetricUtils;
import com.google.common.base.Strings;
import com.singularity.ee.agent.systemagent.api.AManagedMonitor;
import com.sonicsw.mf.common.metrics.IMetric;
import com.sonicsw.mf.common.metrics.IMetricIdentity;
import com.sonicsw.mf.jmx.client.JMSConnectorClient;
import com.sonicsw.mq.common.runtime.IDurableSubscriptionData;
import com.sonicsw.mq.mgmtapi.runtime.IBrokerProxy;
import org.slf4j.LoggerFactory;
import progress.message.ft.ReplicationState;

import javax.management.MalformedObjectNameException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.appdynamics.extensions.util.metrics.MetricConstants.METRICS_SEPARATOR;

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
            IBrokerProxy.QUEUE_MESSAGES_MAXDEPTH_METRIC_ID,
            IBrokerProxy.QUEUE_MESSAGES_RECEIVEDPERSECOND_METRIC_ID,
            IBrokerProxy.QUEUE_MESSAGES_SIZE_METRIC_ID,
    };

    private JMSConnectorClient client;

    public BrokerCollector(JMSConnectorClient client,AManagedMonitor monitor) {
        this.client = client;
        this.monitor = monitor;
    }


    @Override
    void collectAndReport(Configuration config) {
        logger.debug("Getting metrics from Brokers. ");
        Map<String,String> metrics = new HashMap<String, String>();
        Map<String,String> tierMap = config.getTierMap();
        for(ComponentConfig brokerConfig : config.getBrokerConfigs()){
            String tierName = tierMap.get(brokerConfig.getContainerName());
            String metricPrefix = getMetricPrefix(tierName,config.getMetricPrefix());
            if(metricPrefix != null) {
                getMetricsFromBroker(client, config, brokerConfig.getJmxName(), brokerConfig.getName(), metrics,metricPrefix);
            }
            else{
                logger.error("Metric Prefix cannot be formed correctly for {},{},{}",brokerConfig.getName(),brokerConfig.getJmxName(),brokerConfig.getContainerName());
            }
        }
        printMetrics(metrics);
    }




    private void getMetricsFromBroker(JMSConnectorClient client, Configuration config, String brokerJmxName, String brokerName, Map<String, String> metrics, String metricPrefix) {
        try {

            IBrokerProxy proxy = ProxyUtil.getBrokerProxy(client, brokerJmxName);
            metrics.put(metricPrefix + brokerName + METRICS_SEPARATOR + "IsPrimary", getReplicationType(proxy));
            metrics.put(metricPrefix + brokerName + METRICS_SEPARATOR + "IsActive",proxy.getState().toString());
            metrics.put(metricPrefix + brokerName + METRICS_SEPARATOR + "ReplicationState", proxy.getReplicationState().toString());
            if(isReplicationStateActive(proxy)) {
                //set instance metrics
                collectBrokerMetrics(proxy, brokerName, metrics, config.getQueueExcludePatterns(),metricPrefix);
                collectTopicMetrics(proxy, brokerName, metrics, config.getUserExcludePatterns(), config.getTopicExcludePatterns(),metricPrefix);
            }
        }
        catch (MalformedObjectNameException e) {
            logger.error("Failed to create proxy for id '"+ brokerJmxName,e);
        }
        catch(Exception e){
            logger.error("Failed to fetch metrics for " + brokerJmxName,e);
        }
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

    private void collectTopicMetrics(IBrokerProxy proxy, String brokerName, Map<String, String> metrics, List<String> userExcludePatterns, List<String> topicExcludePatterns, String metricPrefix) {
        //get topics for all users
        List<String> users = proxy.getUsersWithDurableSubscriptions(null);
        if(users != null){
            for(String user : users){
                if(!isExcluded(user, userExcludePatterns)){
                    List<IDurableSubscriptionData> topics = proxy.getDurableSubscriptions(user);
                    if(topics != null){
                        for(IDurableSubscriptionData topic : topics){
                            if(!isExcluded(topic.getTopicName(),topicExcludePatterns)){
                                String name = topic.getTopicName();
                                name = Util.replace(name,METRICS_SEPARATOR,"");
                                String topicMetricPrefix = metricPrefix + brokerName + METRICS_SEPARATOR + "users" + METRICS_SEPARATOR +
                                        user + METRICS_SEPARATOR + "topics" + METRICS_SEPARATOR + name + METRICS_SEPARATOR;
                                metrics.put(topicMetricPrefix + "MessageCount", MetricUtils.toWholeNumberString(topic.getMessageCount()));
                                metrics.put(topicMetricPrefix + "TotalMessageSize", MetricUtils.toWholeNumberString(topic.getMessageSize()));
                            }
                        }
                    }
                }
            }
        }
    }

    private void collectBrokerMetrics(IBrokerProxy proxy, String brokerName, Map<String, String> metrics, List<String> excludePatterns, String metricPrefix) {
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
                metricName = metricPrefix + brokerName + METRICS_SEPARATOR + metricName;
                metrics.put(metricName, MetricUtils.toWholeNumberString(m.getValue()));
            }
        }
        logger.debug("*******Metric Details End*********");
        logger.debug("Collected basic broker metrics.");
    }

    public String getMetricName(IMetricIdentity metricIdentity) {
        StringBuffer str = new StringBuffer();
        if(metricIdentity != null && metricIdentity.getNameComponents()!= null){
            for(int i=0;i < metricIdentity.getNameComponents().length; i++){
                String comp = metricIdentity.getNameComponents()[i];
                comp = Util.replace(comp, METRICS_SEPARATOR,"");
                str.append(comp);
                if(i != metricIdentity.getNameComponents().length - 1){
                    str.append(METRICS_SEPARATOR);
                }
            }
            return str.toString();
        }
        logger.warn("Metric not found - " + metricIdentity.getName() + " ; Absolute Name = " + metricIdentity.getAbsoluteName());
        return "";
    }



}
