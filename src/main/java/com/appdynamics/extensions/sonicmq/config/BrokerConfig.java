package com.appdynamics.extensions.sonicmq.config;


public class BrokerConfig {

    private String name;
    private String jmxName;

    public BrokerConfig(String brokerName, String brokerJmxName) {
        this.name = brokerName;
        this.jmxName = brokerJmxName;
    }

    public String getName() {
        return name;
    }

    public String getJmxName() {
        return jmxName;
    }
}
