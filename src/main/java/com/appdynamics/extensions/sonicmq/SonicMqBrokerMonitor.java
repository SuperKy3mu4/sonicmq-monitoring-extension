package com.appdynamics.extensions.sonicmq;


import com.appdynamics.extensions.PathResolver;
import com.appdynamics.extensions.crypto.CryptoUtil;
import com.appdynamics.extensions.file.FileLoader;
import com.appdynamics.extensions.sonicmq.config.Configuration;
import com.appdynamics.extensions.yml.YmlReader;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.singularity.ee.agent.systemagent.api.AManagedMonitor;
import com.singularity.ee.agent.systemagent.api.MetricWriter;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;

import static com.appdynamics.TaskInputArgs.ENCRYPTION_KEY;
import static com.appdynamics.TaskInputArgs.PASSWORD_ENCRYPTED;

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
        if(Strings.isNullOrEmpty(prefix)) {
            config.setMetricPrefix("Custom Metrics|SonicMq|");
        }
        String tierName = System.getProperty(TIER_NAME);
        if(!Strings.isNullOrEmpty(tierName)){
            prefix = prefix.replaceFirst("<TIER_NAME>",tierName);
            config.setMetricPrefix(prefix);
        }

    }

    private void reloadConfig(File file) {
        config = YmlReader.readFromFile(file, Configuration.class);
        if (config != null) {
            setMetricPrefix();
            logger.info("The config file was reloaded successfully.");
        }
        else {
            throw new IllegalArgumentException("The config cannot be initialized from the file " + file.getAbsolutePath());
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
}
