package com.appdynamics.extensions.sonicmq;


import com.google.common.collect.Lists;
import com.sonicsw.mf.common.metrics.IMetricIdentity;
import com.sonicsw.mf.jmx.client.JMSConnectorAddress;
import com.sonicsw.mf.jmx.client.JMSConnectorClient;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Hashtable;

public abstract class Collector {

    public static final String CONNECTION_URLS = "ConnectionURLs";
    public static final String DEFAULT_USER = "DefaultUser";
    public static final String DEFAULT_PASSWORD = "DefaultPassword";
    public static final String METRIC_SEPARATOR = "|";
    protected JMSConnectorClient client;
    public static final Logger logger = Logger.getLogger(SonicMqBrokerMonitor.class);

    public void connect(String location,String username,String password,int timeout)
    {
        if(client != null) {
            return;
        }
        logger.debug("Connecting to '"+ location+"'...");
        client = new JMSConnectorClient();
        Hashtable<String, String> env = new Hashtable<String, String>();
        env.put(CONNECTION_URLS, location);
        env.put(DEFAULT_USER, username);
        env.put(DEFAULT_PASSWORD, password);
        client.connect(new JMSConnectorAddress(env), timeout);
        logger.debug("Connected to '"+ location+"'");
    }

    /**
     * Disconnects the {@link JMSConnectorClient}.
     */
    public void disconnect(String location)
    {
        if(client == null) {
            return;
        }

        logger.debug("Disconnecting from '"+ location+"'...");
        client.disconnect();
        client = null;
        logger.debug("Disconnected from '"+ location+"'");
    }


    public String getMetricName(IMetricIdentity metricIdentity) {
        StringBuffer str = new StringBuffer();
        if(metricIdentity != null && metricIdentity.getNameComponents() != null){
            int len = metricIdentity.getNameComponents().length;
            for(int i=0;i<len; i++){
                str.append(metricIdentity.getNameComponents()[i]);
                if(i < len - 1){
                    str.append(METRIC_SEPARATOR);
                }

            }
            return str.toString();
        }
        logger.info ("Metric Name cannot be created from named components - " + metricIdentity.getName() + " ; Absolute Name = " + metricIdentity.getAbsoluteName());
        return "";
    }



}
