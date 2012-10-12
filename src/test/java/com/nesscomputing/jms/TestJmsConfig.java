/**
 * Copyright (C) 2012 Ness Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

