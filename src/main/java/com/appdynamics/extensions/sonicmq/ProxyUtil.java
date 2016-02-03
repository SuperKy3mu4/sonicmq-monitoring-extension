package com.appdynamics.extensions.sonicmq;

import com.sonicsw.mf.jmx.client.JMSConnectorClient;
import com.sonicsw.mf.mgmtapi.runtime.IAgentManagerProxy;
import com.sonicsw.mf.mgmtapi.runtime.MFProxyFactory;
import com.sonicsw.mq.mgmtapi.runtime.IBrokerProxy;
import com.sonicsw.mq.mgmtapi.runtime.MQProxyFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;


public class ProxyUtil {
    public static IBrokerProxy getBrokerProxy(JMSConnectorClient client, String jmxName) throws MalformedObjectNameException {
        return MQProxyFactory.createBrokerProxy(client, new ObjectName(jmxName));
    }

    public static IAgentManagerProxy getAgentManagerProxy(JMSConnectorClient client, String domain) throws MalformedObjectNameException {
        ObjectName jmxName = new ObjectName(domain + "." + IAgentManagerProxy.GLOBAL_ID + ":ID=" + IAgentManagerProxy.GLOBAL_ID);
        return MFProxyFactory.createAgentManagerProxy(client, jmxName);
    }
}
