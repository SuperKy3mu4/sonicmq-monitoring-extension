package com.appdynamics.extensions.sonicmq;

import com.sonicsw.mf.jmx.client.JMSConnectorAddress;
import com.sonicsw.mf.jmx.client.JMSConnectorClient;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;


public class ConnectionUtil {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(BrokerCollector.class);
    public static final String CONNECTION_URLS = "ConnectionURLs";
    public static final String DEFAULT_USER = "DefaultUser";
    public static final String DEFAULT_PASSWORD = "DefaultPassword";
    public static final String METRIC_SEPARATOR = "|";

    public static void connect(JMSConnectorClient client,String location,String username,String password,int timeout)
    {
        logger.debug("Connecting to '"+ location+"'...");
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
    public static void disconnect(JMSConnectorClient client,String location)
    {
        if(client == null) {
            return;
        }
        logger.debug("Disconnecting from '"+ location+"'...");
        client.disconnect();
        client = null;
        logger.debug("Disconnected from '"+ location+"'");
    }

}
