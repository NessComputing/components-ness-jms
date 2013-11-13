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

import javax.jms.ConnectionFactory;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.inject.CreationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Stage;
import com.google.inject.name.Names;
import com.nesscomputing.config.Config;
import com.nesscomputing.config.ConfigModule;

public class TestJmsModule
{
    @Test(expected=CreationException.class)
    public void testNamedNoEmptyUri()
    {
        Config config = Config.getFixedConfig(ImmutableMap.of("ness.jms.enabled", "true"));

        Guice.createInjector(Stage.PRODUCTION,
                             new ConfigModule(config),
                             new JmsModule(config, "test"));
    }

    @Test
    public void testNamedWorksGlobalDisabled()
    {
        final Config config = Config.getFixedConfig(ImmutableMap.of("ness.jms.enabled", "false"));

        final Injector injector = Guice.createInjector(Stage.PRODUCTION,
                                                       new ConfigModule(config),
                                                       new JmsModule(config, "test"));

        final JmsConfig jmsConfig = injector.getInstance(Key.get(JmsConfig.class, Names.named("test")));
        Assert.assertNotNull(jmsConfig);
        Assert.assertFalse(jmsConfig.isEnabled());
        Assert.assertNull(injector.getExistingBinding(Key.get(ConnectionFactory.class, Names.named("test"))));
    }

    @Test
    public void testNamedWorksLocalDisabled()
    {
        final Config config = Config.getFixedConfig(ImmutableMap.of("ness.jms.enabled", "true",
                                                                                          "ness.jms.test.enabled", "false"));
        final Injector injector = Guice.createInjector(Stage.PRODUCTION,
                                                       new ConfigModule(config),
                                                       new JmsModule(config, "test"));

        final JmsConfig jmsConfig = injector.getInstance(Key.get(JmsConfig.class, Names.named("test")));
        Assert.assertNotNull(jmsConfig);
        Assert.assertFalse(jmsConfig.isEnabled());
        Assert.assertNull(injector.getExistingBinding(Key.get(ConnectionFactory.class, Names.named("test"))));
    }

    @Test
    public void testNamedWorksWithGlobalUri()
    {
        final Config config = Config.getFixedConfig(ImmutableMap.of("ness.jms.test.enabled", "true",
                                                                                          "ness.jms.connection-url", "vm://testbroker?broker.persistent=false"));
        final Injector injector = Guice.createInjector(Stage.PRODUCTION,
                                                       new ConfigModule(config),
                                                       new JmsModule(config, "test"));

        final JmsConfig jmsConfig = injector.getInstance(Key.get(JmsConfig.class, Names.named("test")));
        Assert.assertNotNull(jmsConfig);
        Assert.assertTrue(jmsConfig.isEnabled());

        final ConnectionFactory factory = injector.getInstance(Key.get(ConnectionFactory.class, Names.named("test")));
        Assert.assertNotNull(factory);
    }

    @Test
    public void testNamedWorksWithLocalUri()
    {
        final Config config = Config.getFixedConfig(ImmutableMap.of("ness.jms.test.enabled", "true",
                                                                                          "ness.jms.test.connection-url", "vm://testbroker?broker.persistent=false"));
        final Injector injector = Guice.createInjector(Stage.PRODUCTION,
                                                       new ConfigModule(config),
                                                       new JmsModule(config, "test"));

        final JmsConfig jmsConfig = injector.getInstance(Key.get(JmsConfig.class, Names.named("test")));
        Assert.assertNotNull(jmsConfig);
        Assert.assertTrue(jmsConfig.isEnabled());

        final ConnectionFactory factory = injector.getInstance(Key.get(ConnectionFactory.class, Names.named("test")));
        Assert.assertNotNull(factory);
    }

    @Test(expected=CreationException.class)
    public void testNoEmptyUri()
    {
        final Config config = Config.getFixedConfig(ImmutableMap.of("ness.jms.enabled", "true"));

        Guice.createInjector(Stage.PRODUCTION,
                             new ConfigModule(config),
                             new JmsModule(config, "test"));
    }

    @Test
    public void testWorksGlobalDisabled()
    {
        final Config config = Config.getFixedConfig(ImmutableMap.of("ness.jms.enabled", "false"));

        final Injector injector = Guice.createInjector(Stage.PRODUCTION,
                                                       new ConfigModule(config),
                                                       new JmsModule(config, "test"));

        final JmsConfig jmsConfig = injector.getInstance(Key.get(JmsConfig.class, Names.named("test")));
        Assert.assertNotNull(jmsConfig);
        Assert.assertFalse(jmsConfig.isEnabled());
        Assert.assertNull(injector.getExistingBinding(Key.get(ConnectionFactory.class)));
    }

    @Test
    public void testWorksWithGlobalUri()
    {
        final Config config = Config.getFixedConfig(ImmutableMap.of("ness.jms.enabled", "true",
              "ness.jms.connection-url", "vm://testbroker?broker.persistent=false"));
        final Injector injector = Guice.createInjector(Stage.PRODUCTION,
                                                       new ConfigModule(config),
                                                       new JmsModule(config, "test"));

        final JmsConfig jmsConfig = injector.getInstance(Key.get(JmsConfig.class, Names.named("test")));
        Assert.assertNotNull(jmsConfig);
        Assert.assertTrue(jmsConfig.isEnabled());

        final ConnectionFactory factory = injector.getInstance(Key.get(ConnectionFactory.class, Names.named("test")));
        Assert.assertNotNull(factory);
    }

    @Test
    public void testMultipleModules()
    {
        final Config config = Config.getFixedConfig(ImmutableMap.of("ness.jms.enabled", "true",
                "ness.jms.connection-url", "vm://testbroker?broker.persistent=false"));

        final Injector injector = Guice.createInjector(Stage.PRODUCTION,
                new ConfigModule(config),
                new JmsModule(config, "test"),
                new JmsModule(config, "test2"));

        final JmsConfig jmsConfig = injector.getInstance(Key.get(JmsConfig.class, Names.named("test2")));
        Assert.assertNotNull(jmsConfig);
        Assert.assertTrue(jmsConfig.isEnabled());

        final ConnectionFactory factory = injector.getInstance(Key.get(ConnectionFactory.class, Names.named("test2")));
        Assert.assertNotNull(factory);

        Assert.assertNotNull(injector.getInstance(Key.get(ConnectionFactory.class, Names.named("test"))));
    }
}
