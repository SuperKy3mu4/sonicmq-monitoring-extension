package com.appdynamics.extensions.sonicmq;


import com.appdynamics.extensions.sonicmq.config.Configuration;
import com.appdynamics.extensions.util.MetricUtils;
import com.google.common.base.Strings;
import com.sonicsw.mf.common.metrics.IMetric;
import com.sonicsw.mf.common.metrics.IMetricIdentity;
import com.sonicsw.mf.jmx.client.JMSConnectorClient;
import com.sonicsw.mq.mgmtapi.runtime.IBrokerProxy;
import com.sonicsw.mq.mgmtapi.runtime.MQProxyFactory;
import org.apache.log4j.Logger;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.HashMap;
import java.util.Map;

public class BrokerCollector extends Collector{

    private Configuration config;
    private static final Logger logger = Logger.getLogger(BrokerCollector.class);

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

    public BrokerCollector(Configuration config){
        this.config = config;
    }


    public Map<String, String> getMetrics() {
        logger.debug("Getting metrics from Broker. ");
        Map<String,String> metrics = new HashMap<String, String>();
        try{
            //connect JMX
            connect(config.getLocation(),config.getUsername(),config.getPassword(),config.getTimeout());
            IBrokerProxy proxy = getProxy(client,new ObjectName(config.getBrokerDomain()));
            proxy.enableMetrics(metricIds);
            IMetric[] data = proxy.getMetricsData(metricIds, false).getMetrics();
            for(IMetric m : data) {
                String metricName = getMetricName(m.getMetricIdentity());
                if(!Strings.isNullOrEmpty(metricName)) {
                    metrics.put(metricName, MetricUtils.toWholeNumberString(m.getValue()));
                }
            }
        } catch (MalformedObjectNameException e) {
            logger.error("Failed to create proxy for id '"+ config.getBrokerDomain() +"': "+e.getMessage());
        }
        finally{
            disconnect(config.getLocation());
        }
        logger.debug("Collected Broker metrics");
        return metrics;
    }

    protected final IBrokerProxy getProxy(JMSConnectorClient client, ObjectName jmxName) {
        return MQProxyFactory.createBrokerProxy(client, jmxName);
    }

}
