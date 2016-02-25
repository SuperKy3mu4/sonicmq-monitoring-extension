package com.appdynamics.extensions.sonicmq.config;


import com.appdynamics.extensions.yml.YmlReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;

public class ConfigUtilTest {

    private Configuration config;
    @Test
    public void canLoadConfigFile() throws FileNotFoundException {
        config = YmlReader.readFromFile(this.getClass().getResource("/conf/config.yml").getFile(), Configuration.class);
        Assert.assertTrue(config != null);
    }

    @Test
    public void replaceTierNameInMetricPrefix() {
        String prefix = "Server|Component:<TIER_NAME>|Custom Metrics|SonicMq";
        prefix = prefix.replaceFirst("<TIER_NAME>","Hello");
        Assert.assertTrue(prefix.equals("Server|Component:Hello|Custom Metrics|SonicMq"));
    }

}
