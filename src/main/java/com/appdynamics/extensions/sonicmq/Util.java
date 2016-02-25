package com.appdynamics.extensions.sonicmq;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Util {

    public static final String TIER_NAME = "-Dappdynamics.agent.tierName=";

    public static String getHostname() {
        String hostname;
        try {
            // See http://stackoverflow.com/questions/7348711/recommended-way-to-get-hostname-in-java
            // for information on when this will fail.
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException needToAskSystem) {
            hostname = null;
        }
        return hostname;
    }

    public static String getTierName(String jvmArgs) {
        if(jvmArgs == null){
            return null;
        }
        String tierName = null;
        int idx = jvmArgs.indexOf(TIER_NAME);
        if(idx != -1){
            String tierArg = jvmArgs.substring(idx ,jvmArgs.indexOf(" ",idx));
            if(tierArg.indexOf("=") != -1){
                tierName = tierArg.substring(tierArg.indexOf("=") + 1);
            }
        }
        return tierName;
    }
}
