package com.appdynamics.extensions.sonicmq.config;


import java.util.List;

public class Configuration {

    private String location;
    private String username;
    private String password;
    private int timeout;
    private List<String> queueExcludePatterns;
    private List<String> userExcludePatterns;
    private List<String> topicExcludePatterns;
    private String metricPrefix;
    private String domain;

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

    public String getMetricPrefix() {
        return metricPrefix;
    }

    public List<String> getQueueExcludePatterns() {
        return queueExcludePatterns;
    }

    public void setQueueExcludePatterns(List<String> queueExcludePatterns) {
        this.queueExcludePatterns = queueExcludePatterns;
    }

    public void setMetricPrefix(String metricPrefix) {
        this.metricPrefix = metricPrefix;
    }

    public List<String> getUserExcludePatterns() {
        return userExcludePatterns;
    }

    public void setUserExcludePatterns(List<String> userExcludePatterns) {
        this.userExcludePatterns = userExcludePatterns;
    }

    public List<String> getTopicExcludePatterns() {
        return topicExcludePatterns;
    }

    public void setTopicExcludePatterns(List<String> topicExcludePatterns) {
        this.topicExcludePatterns = topicExcludePatterns;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }
}
