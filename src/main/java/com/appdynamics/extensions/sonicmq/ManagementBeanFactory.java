package com.appdynamics.extensions.sonicmq;


import com.sonicsw.ma.mgmtapi.config.MgmtException;
import com.sonicsw.mq.mgmtapi.config.MQMgmtBeanFactory;
import com.sonicsw.mx.config.ConfigServiceException;
import org.slf4j.LoggerFactory;

public class ManagementBeanFactory {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ManagementBeanFactory.class);

    public static void connect(MQMgmtBeanFactory factory ,String location,String domain,String username,String password) throws MgmtException {
        logger.debug("Connecting to Management bean factory location : {}",location);
        factory.connect(domain, location, username, password);
    }

    public static void disconnect(MQMgmtBeanFactory factory) throws ConfigServiceException {
        if(factory != null){
            factory.disconnect();
        }
    }
}
