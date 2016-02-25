package com.appdynamics.extensions.sonicmq;


import com.appdynamics.extensions.sonicmq.config.Configuration;
import com.google.common.base.Strings;
import com.singularity.ee.agent.systemagent.api.AManagedMonitor;
import com.singularity.ee.agent.systemagent.api.MetricWriter;
import org.slf4j.LoggerFactory;

import java.util.Map;
import static com.appdynamics.extensions.util.metrics.MetricConstants.METRICS_SEPARATOR;


public abstract class Collector {
    public static final String DEFAULT_METRIC_PREFIX = "Custom Metrics|SonicMq|";
    protected AManagedMonitor monitor;

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Collector.class);

    abstract void  collectAndReport(Configuration config);

    public void printMetrics(Map<String, String> metrics) {
        for(Map.Entry<String,String> entry : metrics.entrySet()){
            printAverageAverageIndividual(entry.getKey(), entry.getValue());
        }
    }


    private void printAverageAverageIndividual(String metricPath, String metricValue) {
        printMetric(metricPath, metricValue,
                MetricWriter.METRIC_AGGREGATION_TYPE_AVERAGE,
                MetricWriter.METRIC_TIME_ROLLUP_TYPE_AVERAGE,
                MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_INDIVIDUAL
        );
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
        MetricWriter metricWriter = monitor.getMetricWriter(metricPath,
                aggType,
                timeRollupType,
                clusterRollupType
        );
        //System.out.println(getLogPrefix()+"Sending [" + aggType + METRIC_SEPARATOR + timeRollupType + METRIC_SEPARATOR + clusterRollupType
        //            + "] metric = " + metricPath + " = " + metricValue);
        if (logger.isDebugEnabled()) {
            logger.debug("Sending [" + aggType + METRICS_SEPARATOR + timeRollupType + METRICS_SEPARATOR + clusterRollupType
                    + "] metric = " + metricPath + " = " + metricValue);
        }
        metricWriter.printMetric(metricValue);
    }

    String getMetricPrefix(String tierName, String metricPrefix) {
        if(Strings.isNullOrEmpty(metricPrefix)) {
            return DEFAULT_METRIC_PREFIX;
        }
        if(!Strings.isNullOrEmpty(tierName)){
            metricPrefix = metricPrefix.replaceFirst("<TIER_NAME>",tierName);
            logger.debug("Changed using Sonic JVM argument metric prefix = {}",metricPrefix);
            return metricPrefix;
        }
        return null;
    }
}
