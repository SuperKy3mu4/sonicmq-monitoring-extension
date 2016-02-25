package com.appdynamics.extensions.sonicmq;


import com.appdynamics.extensions.sonicmq.config.ComponentConfig;
import com.appdynamics.extensions.sonicmq.config.Configuration;
import com.appdynamics.extensions.util.MetricUtils;
import com.google.common.base.Strings;
import com.singularity.ee.agent.systemagent.api.AManagedMonitor;
import com.sonicsw.mf.common.metrics.IMetric;
import com.sonicsw.mf.common.metrics.IMetricIdentity;
import com.sonicsw.mf.jmx.client.JMSConnectorClient;
import com.sonicsw.mf.mgmtapi.runtime.IAgentManagerProxy;
import com.sonicsw.mf.mgmtapi.runtime.IAgentProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import java.util.HashMap;
import java.util.Map;

import static com.appdynamics.extensions.util.metrics.MetricConstants.METRICS_SEPARATOR;

public class ContainerCollector extends Collector{

    private static final Logger logger = LoggerFactory.getLogger(ContainerCollector.class);

    private static final IMetricIdentity[] metricIds = new IMetricIdentity[] {
            IAgentProxy.SYSTEM_MEMORY_MAXUSAGE_METRIC_ID,
            IAgentProxy.SYSTEM_MEMORY_CURRENTUSAGE_METRIC_ID,
            IAgentProxy.SYSTEM_THREADS_CURRENTTOTAL_METRIC_ID,
            IAgentProxy.SYSTEM_THREADS_CURRENTPOOLSIZE_METRIC_ID,
            IAgentProxy.SYSTEM_THREADS_MAXPOOLSIZE_METRIC_ID,
            IAgentProxy.SYSTEM_THREADS_POOLWAITS_METRIC_ID
    };

    private JMSConnectorClient client;

    public ContainerCollector(JMSConnectorClient client,AManagedMonitor monitor) {
        this.monitor = monitor;
        this.client = client;
    }


    public void collectAndReport(Configuration config) {
        logger.debug("Getting metrics from Container. ");
        Map<String,String> metrics = new HashMap<String, String>();
        Map<String,String> tierMap = config.getTierMap();
        for(ComponentConfig containerConfig : config.getContainerConfigs()){
            String tierName = tierMap.get(containerConfig.getContainerName());
            String metricPrefix = getMetricPrefix(tierName,config.getMetricPrefix());
            if(metricPrefix != null) {
                getMetricsFromContainer(client, containerConfig.getJmxName(), containerConfig.getName(), metrics, metricPrefix);
            }
            else{
                logger.error("Metric Prefix cannot be formed correctly for {},{},{}",containerConfig.getName(),containerConfig.getJmxName(),containerConfig.getContainerName());
            }
        }
        printMetrics(metrics);
    }

    private void getMetricsFromContainer(JMSConnectorClient client, String jmxName, String name, Map<String, String> metrics, String metricPrefix) {
        try {

            IAgentManagerProxy proxy = ProxyUtil.getAgentManagerProxy(client, jmxName);
            collectContainerMetrics(proxy, name, metrics,metricPrefix);
        }
        catch (MalformedObjectNameException e) {
            logger.error("Failed to create proxy for id '"+ jmxName,e);
        }
        catch(Exception e){
            logger.error("Failed to fetch metrics for " + jmxName,e);
        }
    }


    private void collectContainerMetrics(IAgentManagerProxy proxy, String name, Map<String, String> metrics, String metricPrefix) {
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
            if (!Strings.isNullOrEmpty(metricName)) {
                logger.debug("Metric Name : {} ,Metric Value : {}",metricName,m.getValue());
                metricName = metricPrefix + METRICS_SEPARATOR + name + METRICS_SEPARATOR + metricName;
                metrics.put(metricName, MetricUtils.toWholeNumberString(m.getValue()));
            }
        }
        logger.debug("*******Metric Details End*********");
        logger.debug("Collected container metrics.");
    }


    public String getMetricName(IMetricIdentity metricIdentity) {
        StringBuffer str = new StringBuffer();
        if(metricIdentity != null && metricIdentity.getNameComponents()!= null){
            for(int i=0;i < metricIdentity.getNameComponents().length; i++){
                str.append(metricIdentity.getNameComponents()[i]);
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
