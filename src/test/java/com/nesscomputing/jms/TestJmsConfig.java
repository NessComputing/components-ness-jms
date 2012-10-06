package com.nesscomputing.jms;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;

import com.nesscomputing.config.Config;

public class TestJmsConfig
{
    @Test
    public void testDefault()
    {
        final Config config = Config.getEmptyConfig();
        final JmsConfig jmsConfig = config.getBean(JmsConfig.class);

        Assert.assertTrue(jmsConfig.isEnabled());
    }

    @Test
    public void testDisabled()
    {
        final Config config = Config.getFixedConfig("ness.jms.enabled", "false");
        final JmsConfig jmsConfig = config.getBean(JmsConfig.class);

        Assert.assertFalse(jmsConfig.isEnabled());
    }

    @Test
    public void testEnabled()
    {
        final Config config = Config.getFixedConfig("ness.jms.enabled", "true");
        final JmsConfig jmsConfig = config.getBean(JmsConfig.class);

        Assert.assertTrue(jmsConfig.isEnabled());
    }

    @Test
    public void testGlobalEnabledLocalDisabled()
    {
        final Config config = Config.getFixedConfig("ness.jms.enabled", "true",
                                                    "ness.jms.user-friendship.enabled", "false");
        final JmsConfig jmsConfig = config.getBean(JmsConfig.class, ImmutableMap.of("name", "user-friendship"));

        Assert.assertFalse(jmsConfig.isEnabled());
    }

    @Test
    public void testGlobalDisabledLocalEnabled()
    {
        final Config config = Config.getFixedConfig("ness.jms.enabled", "false",
                                                    "ness.jms.user-friendship.enabled", "true");
        final JmsConfig jmsConfig = config.getBean(JmsConfig.class, ImmutableMap.of("name", "user-friendship"));

        Assert.assertTrue(jmsConfig.isEnabled());
    }
}

