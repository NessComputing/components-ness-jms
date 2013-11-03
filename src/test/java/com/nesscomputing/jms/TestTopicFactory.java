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

import static java.lang.String.format;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.google.inject.name.Named;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.nesscomputing.config.Config;
import com.nesscomputing.config.ConfigModule;
import com.nesscomputing.jms.util.CountingMessageCallback;
import com.nesscomputing.jms.util.DummyMessageCallback;
import com.nesscomputing.testing.lessio.AllowDNSResolution;
import com.nesscomputing.testing.lessio.AllowNetworkAccess;
import com.nesscomputing.testing.lessio.AllowNetworkListen;

@AllowDNSResolution
@AllowNetworkListen(ports= {0})
@AllowNetworkAccess(endpoints= {"*:*"})
public class TestTopicFactory
{
    @Inject
    @Named("test")
    public JmsRunnableFactory topicRunnableFactory;

    private static Connection CONNECTION = null;
    private static String BROKER_URI = null;

    @BeforeClass
    public static void startBroker() throws Exception
    {
        BROKER_URI = format("vm:broker:(vm://testbroker-%s)?persistent=false&useJmx=false", UUID.randomUUID().toString());
        final ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URI);
        Assert.assertNull(CONNECTION);
        CONNECTION = connectionFactory.createConnection();
        Thread.sleep(2000L);
    }

    @AfterClass
    public static void shutdownBroker() throws Exception
    {
        Assert.assertNotNull(CONNECTION);
        CONNECTION.close();
        CONNECTION = null;
    }

    @Before
    public void setUp()
    {
        final Config config = Config.getFixedConfig(ImmutableMap.of("ness.jms.test.enabled", "true",
                                                                                          "ness.jms.test.connection-url", BROKER_URI));
        final Injector injector = Guice.createInjector(Stage.PRODUCTION,
                                                       new ConfigModule(config),
                                                       new JmsModule(config, "test"));

        injector.injectMembers(this);

        Assert.assertNotNull(topicRunnableFactory);
    }

    @Test
    public void testSimpleConsumer() throws Exception
    {
        final TopicConsumer topicConsumer = topicRunnableFactory.createTopicListener("test-topic", new DummyMessageCallback());
        Assert.assertNotNull(topicConsumer);
    }

    @Test
    public void testSimpleProducer() throws Exception
    {
        final TopicProducer<Object> topicProducer = topicRunnableFactory.createTopicJsonProducer("test-topic");
        Assert.assertNotNull(topicProducer);
    }

    @Test
    public void stopConsumerWithNoMessages() throws Exception
    {
        final TopicConsumer topicConsumer = topicRunnableFactory.createTopicListener("test-topic", new DummyMessageCallback());
        Assert.assertNotNull(topicConsumer);

        final Thread topicThread = new Thread(topicConsumer);
        topicThread.start();

        Thread.sleep(2000L);
        Assert.assertTrue(topicConsumer.isConnected());

        topicConsumer.shutdown();
        topicThread.interrupt();
        topicThread.join();
    }

    @Test
    public void stopProducerWithNoMessages() throws Exception
    {
        final TopicProducer<Object>topicProducer = topicRunnableFactory.createTopicJsonProducer("test-topic");
        Assert.assertNotNull(topicProducer);

        final Thread topicThread = new Thread(topicProducer);
        topicThread.start();

        Thread.sleep(2000L);
        Assert.assertFalse(topicProducer.isConnected());

        topicProducer.shutdown();
        topicThread.interrupt();
        topicThread.join();
    }

    @Test
    public void testProduceConsume() throws Exception
    {
        final CountingMessageCallback cmc = new CountingMessageCallback();
        final TopicConsumer topicConsumer = topicRunnableFactory.createTopicListener("test-topic", cmc);
        final TopicProducer<Object> topicProducer = topicRunnableFactory.createTopicJsonProducer("test-topic");
        final Thread consumerThread = new Thread(topicConsumer);
        final Thread producerThread = new Thread(topicProducer);
        consumerThread.start();
        producerThread.start();

        Thread.sleep(1000L);

        Assert.assertTrue(topicConsumer.isConnected());
        Assert.assertFalse(topicProducer.isConnected());

        final int maxCount = 1000;
        for (int i = 0; i < maxCount; i++) {
            Assert.assertTrue(topicProducer.offerWithTimeout(format("hello, world %d", i), 1, TimeUnit.SECONDS));
        }

        for (int i = 0; i < 100 && !topicProducer.isEmpty(); i++) {
            Thread.sleep(10L);
        }

        Thread.sleep(100L);

        Assert.assertTrue(topicProducer.isEmpty());
        Assert.assertEquals(maxCount, cmc.getCount());

        topicProducer.shutdown();
        topicConsumer.shutdown();
        producerThread.interrupt();
        consumerThread.interrupt();
        producerThread.join();
        consumerThread.join();
    }

    @Test
    public void testOneProducerTwoConsumers() throws Exception
    {
        final CountingMessageCallback cmc1 = new CountingMessageCallback();
        final CountingMessageCallback cmc2 = new CountingMessageCallback();
        final TopicConsumer topicConsumer1 = topicRunnableFactory.createTopicListener("test-topic", cmc1);
        final TopicConsumer topicConsumer2 = topicRunnableFactory.createTopicListener("test-topic", cmc2);
        final TopicProducer<Object> topicProducer = topicRunnableFactory.createTopicJsonProducer("test-topic");
        final Thread consumerThread1 = new Thread(topicConsumer1);
        final Thread consumerThread2 = new Thread(topicConsumer2);
        final Thread producerThread = new Thread(topicProducer);
        consumerThread1.start();
        consumerThread2.start();
        producerThread.start();

        Thread.sleep(1000L);

        Assert.assertTrue(topicConsumer1.isConnected());
        Assert.assertTrue(topicConsumer2.isConnected());
        Assert.assertFalse(topicProducer.isConnected());

        final int maxCount = 1000;
        for (int i = 0; i < maxCount; i++) {
            Assert.assertTrue(topicProducer.offerWithTimeout(format("hello, world %d", i), 1, TimeUnit.SECONDS));
        }

        for (int i = 0; i < 100 && !topicProducer.isEmpty(); i++) {
            Thread.sleep(10L);
        }
        Assert.assertTrue(topicProducer.isEmpty());
        Assert.assertEquals(maxCount, cmc1.getCount());
        Assert.assertEquals(maxCount, cmc2.getCount());

        topicProducer.shutdown();
        topicConsumer1.shutdown();
        topicConsumer2.shutdown();
        producerThread.interrupt();
        consumerThread1.interrupt();
        consumerThread2.interrupt();
        producerThread.join();
        consumerThread1.join();
        consumerThread2.join();
    }

    @Test
    public void testTwoProducersOneConsumer() throws Exception
    {
        final CountingMessageCallback cmc = new CountingMessageCallback();
        final TopicConsumer topicConsumer = topicRunnableFactory.createTopicListener("test-topic", cmc);
        final TopicProducer<Object> topicProducer1 = topicRunnableFactory.createTopicJsonProducer("test-topic");
        final TopicProducer<Object> topicProducer2 = topicRunnableFactory.createTopicJsonProducer("test-topic");
        final Thread consumerThread = new Thread(topicConsumer);
        final Thread producerThread1 = new Thread(topicProducer1);
        final Thread producerThread2 = new Thread(topicProducer2);

        consumerThread.start();
        producerThread1.start();
        producerThread2.start();

        Thread.sleep(1000L);

        Assert.assertTrue(topicConsumer.isConnected());
        Assert.assertFalse(topicProducer1.isConnected());
        Assert.assertFalse(topicProducer2.isConnected());

        final int maxCount = 1000;
        for (int i = 0; i < maxCount; i++) {
            Assert.assertTrue(topicProducer1.offerWithTimeout(format("hello, world %d", i), 1, TimeUnit.SECONDS));
            Assert.assertTrue(topicProducer2.offerWithTimeout(format("hello, wold %d", i), 1, TimeUnit.SECONDS));
        }

        for (int i = 0; i < 100 && !(topicProducer1.isEmpty() && topicProducer2.isEmpty()); i++) {
            Thread.sleep(10L);
        }
        Thread.sleep(100L);

        Assert.assertTrue(topicProducer1.isEmpty());
        Assert.assertTrue(topicProducer2.isEmpty());
        Assert.assertEquals(maxCount*2, cmc.getCount());

        topicProducer1.shutdown();
        topicProducer2.shutdown();
        topicConsumer.shutdown();
        producerThread1.interrupt();
        producerThread2.interrupt();
        consumerThread.interrupt();
        producerThread1.join();
        producerThread2.join();
        consumerThread.join();
    }

    @Test
    public void testTwoProducersTwoConsumers() throws Exception
    {
        final CountingMessageCallback cmc1 = new CountingMessageCallback();
        final CountingMessageCallback cmc2 = new CountingMessageCallback();
        final TopicConsumer topicConsumer1 = topicRunnableFactory.createTopicListener("test-topic", cmc1);
        final TopicConsumer topicConsumer2 = topicRunnableFactory.createTopicListener("test-topic", cmc2);
        final TopicProducer<Object> topicProducer1 = topicRunnableFactory.createTopicJsonProducer("test-topic");
        final TopicProducer<Object> topicProducer2 = topicRunnableFactory.createTopicJsonProducer("test-topic");
        final Thread consumerThread1 = new Thread(topicConsumer1);
        final Thread consumerThread2 = new Thread(topicConsumer2);
        final Thread producerThread1 = new Thread(topicProducer1);
        final Thread producerThread2 = new Thread(topicProducer2);

        consumerThread1.start();
        consumerThread2.start();
        producerThread1.start();
        producerThread2.start();

        Thread.sleep(1000L);

        Assert.assertTrue(topicConsumer1.isConnected());
        Assert.assertTrue(topicConsumer2.isConnected());
        Assert.assertFalse(topicProducer1.isConnected());
        Assert.assertFalse(topicProducer2.isConnected());

        final int maxCount = 1000;
        for (int i = 0; i < maxCount; i++) {
            Assert.assertTrue(topicProducer1.offerWithTimeout(format("hello, world %d", i), 1, TimeUnit.SECONDS));
            Assert.assertTrue(topicProducer2.offerWithTimeout(format("hello, wold %d", i), 1, TimeUnit.SECONDS));
        }

        for (int i = 0; i < 100 && !(topicProducer1.isEmpty() && topicProducer2.isEmpty()); i++) {
            Thread.sleep(10L);
        }
        Thread.sleep(100L);

        Assert.assertTrue(topicProducer1.isEmpty());
        Assert.assertTrue(topicProducer2.isEmpty());

        // Hooray, it is a message bus.
        Assert.assertEquals(maxCount*2, cmc1.getCount());
        Assert.assertEquals(maxCount*2, cmc2.getCount());

        topicProducer1.shutdown();
        topicProducer2.shutdown();
        topicConsumer1.shutdown();
        topicConsumer2.shutdown();
        producerThread1.interrupt();
        producerThread2.interrupt();
        consumerThread1.interrupt();
        consumerThread2.interrupt();
        producerThread1.join();
        producerThread2.join();
        consumerThread1.join();
        consumerThread2.join();
    }
}



