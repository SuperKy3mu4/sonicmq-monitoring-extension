package com.appdynamics.extensions.sonicmq.config;


import com.appdynamics.extensions.yml.YmlReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;

public class ConfigUtilTest {


    @Test
    public void canLoadConfigFile() throws FileNotFoundException {
        Configuration config = YmlReader.readFromFile(this.getClass().getResource("/conf/config.yml").getFile(), Configuration.class);
        Assert.assertTrue(config != null);
    }
}