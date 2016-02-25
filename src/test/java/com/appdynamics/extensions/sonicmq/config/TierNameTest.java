package com.appdynamics.extensions.sonicmq.config;

import com.appdynamics.extensions.sonicmq.Util;
import org.junit.Assert;
import org.junit.Test;

public class TierNameTest {

    @Test
    public void testTierNameWithJVMArgs(){
        String jvmArgs = "-Xms64m -Xmx512m -javaagent:/opt/appdynamics/java-agent/javaagent.jar -Dappdynamics.agent.applicationName=load_Parks -Dappdynamics.controller.hostName=disney-preprod.saas.appdynamics.com -Dappdynamics.agent.tierName=mmp_sonic_HostManagers -Dappdynamics.agent.nodeName=nl-fldi-00922_2608_nl-fldi-00922";
        String tierName = Util.getTierName(jvmArgs);
        Assert.assertTrue(tierName.equals("mmp_sonic_HostManagers"));
    }
}
