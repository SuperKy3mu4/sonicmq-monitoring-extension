package com.appdynamics.extensions.sonicmq.config;


public class Configuration {

    private String location;
    private String username;
    private String password;
    private String brokerDomain;
    private String containerDomain;
    private int timeout;

    private String metricPrefix;

    public String getBrokerDomain() {
        return brokerDomain;
    }

    public void setBrokerDomain(String brokerDomain) {
        this.brokerDomain = brokerDomain;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getContainerDomain() {
        return containerDomain;
    }

    public void setContainerDomain(String containerDomain) {
        this.containerDomain = containerDomain;
    }

    public String getMetricPrefix() {
        return metricPrefix;
    }

    public void setMetricPrefix(String metricPrefix) {
        this.metricPrefix = metricPrefix;
    }
}
