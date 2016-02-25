package com.appdynamics.extensions.sonicmq.config;


public class ComponentConfig {

    private String name;
    private String jmxName;
    private String containerName;

    public ComponentConfig(String brokerName, String brokerJmxName,String containerName) {
        this.name = brokerName;
        this.jmxName = brokerJmxName;
        this.containerName = containerName;
    }

    public String getName() {
        return name;
    }

    public String getJmxName() {
        return jmxName;
    }


    public String getContainerName() {
        return containerName;
    }
}
