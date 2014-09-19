package com.appdynamics.extensions.sonicmq;


import com.appdynamics.extensions.sonicmq.config.Configuration;
import com.appdynamics.extensions.util.MetricUtils;
import com.google.common.base.Strings;
import com.sonicsw.mf.common.metrics.IMetric;
import com.sonicsw.mf.common.metrics.IMetricIdentity;
import com.sonicsw.mf.jmx.client.JMSConnectorClient;
import com.sonicsw.mf.mgmtapi.runtime.IAgentProxy;
import com.sonicsw.mf.mgmtapi.runtime.MFProxyFactory;
import com.sonicsw.mq.mgmtapi.runtime.IBrokerProxy;
import com.sonicsw.mq.mgmtapi.runtime.MQProxyFactory;
import org.apache.log4j.Logger;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.HashMap;
import java.util.Map;

public class ContainerCollector extends Collector{

    private Configuration config;
    private static final Logger logger = Logger.getLogger(ContainerCollector.class);

    private static final IMetricIdentity[] metricIds = new IMetricIdentity[] {
            IAgentProxy.SYSTEM_MEMORY_MAXUSAGE_METRIC_ID,
            IAgentProxy.SYSTEM_MEMORY_CURRENTUSAGE_METRIC_ID,
            IAgentProxy.SYSTEM_THREADS_CURRENTTOTAL_METRIC_ID,
            IAgentProxy.SYSTEM_THREADS_CURRENTPOOLSIZE_METRIC_ID,
            IAgentProxy.SYSTEM_THREADS_MAXPOOLSIZE_METRIC_ID,
            IAgentProxy.SYSTEM_THREADS_POOLWAITS_METRIC_ID
    };

    public ContainerCollector(Configuration config){
        this.config = config;
    }


    public Map<String, String> getMetrics() {
        logger.debug("Getting metrics from Container. ");
        Map<String,String> metrics = new HashMap<String, String>();
        try{
            //connect JMX
            connect(config.getLocation(),config.getUsername(),config.getPassword(),config.getTimeout());
            IAgentProxy proxy = getProxy(client,new ObjectName(config.getContainerDomain()));
            proxy.enableMetrics(metricIds);
            IMetric[] data = proxy.getMetricsData(metricIds, false).getMetrics();
            for(IMetric m : data) {
                String metricName = getMetricName(m.getMetricIdentity());
                if(!Strings.isNullOrEmpty(metricName)) {
                    metrics.put(metricName, MetricUtils.toWholeNumberString(m.getValue()));
                }
            }
        } catch (MalformedObjectNameException e) {
            logger.error("Failed to create proxy for id '"+ config.getContainerDomain() +"': "+e.getMessage());
        }
        finally{
            disconnect(config.getLocation());
        }
        logger.debug("Collected Container metrics");
        return metrics;
    }



    protected final IAgentProxy getProxy(JMSConnectorClient client, ObjectName jmxName) {
        return MFProxyFactory.createAgentProxy(client, jmxName);
    }

}
