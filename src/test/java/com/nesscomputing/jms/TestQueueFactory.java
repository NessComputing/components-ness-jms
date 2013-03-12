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
import com.nesscomputing.testing.lessio.AllowNetworkListen;

@AllowDNSResolution
@AllowNetworkListen(ports={0})
public class TestQueueFactory
{
    @Inject
    @Named("test")
    public JmsRunnableFactory queueRunnableFactory;

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

        Assert.assertNotNull(queueRunnableFactory);
    }

    @Test
    public void testSimpleConsumer() throws Exception
    {
        final QueueConsumer queueConsumer = queueRunnableFactory.createQueueListener("test-queue", new DummyMessageCallback());
        Assert.assertNotNull(queueConsumer);
    }

    @Test
    public void testSimpleProducer() throws Exception
    {
        final QueueProducer<Object> queueProducer = queueRunnableFactory.createQueueJsonMessageProducer("test-queue");
        Assert.assertNotNull(queueProducer);
    }

    @Test
    public void stopConsumerWithNoMessages() throws Exception
    {
        final QueueConsumer queueConsumer = queueRunnableFactory.createQueueListener("test-queue", new DummyMessageCallback());
        Assert.assertNotNull(queueConsumer);

        final Thread queueThread = new Thread(queueConsumer);
        queueThread.start();

        Thread.sleep(2000L);
        Assert.assertTrue(queueConsumer.isConnected());

        queueConsumer.shutdown();
        queueThread.interrupt();
        queueThread.join();
    }

    @Test
    public void stopProducerWithNoMessages() throws Exception
    {
        final QueueProducer<Object> queueProducer = queueRunnableFactory.createQueueJsonMessageProducer("test-queue");
        Assert.assertNotNull(queueProducer);

        final Thread queueThread = new Thread(queueProducer);
        queueThread.start();

        Thread.sleep(2000L);
        Assert.assertFalse(queueProducer.isConnected());

        queueProducer.shutdown();
        queueThread.interrupt();
        queueThread.join();
    }

    @Test
    public void testProduceConsume() throws Exception
    {
        final CountingMessageCallback cmc = new CountingMessageCallback();
        final QueueConsumer queueConsumer = queueRunnableFactory.createQueueListener("test-queue", cmc);
        final QueueProducer<Object> queueProducer = queueRunnableFactory.createQueueJsonMessageProducer("test-queue");
        final Thread consumerThread = new Thread(queueConsumer);
        final Thread producerThread = new Thread(queueProducer);
        consumerThread.start();
        producerThread.start();

        Thread.sleep(1000L);

        Assert.assertTrue(queueConsumer.isConnected());
        Assert.assertFalse(queueProducer.isConnected());

        final int maxCount = 1000;
        for (int i = 0; i < maxCount; i++) {
            queueProducer.put(format("hello, world %d", i));
        }

        for (int i = 0; i < 100 && !queueProducer.isEmpty(); i++) {
            Thread.sleep(10L);
        }
        Assert.assertTrue(queueProducer.isEmpty());
        Assert.assertEquals(maxCount, cmc.getCount());

        queueProducer.shutdown();
        queueConsumer.shutdown();
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
        final QueueConsumer queueConsumer1 = queueRunnableFactory.createQueueListener("test-queue", cmc1);
        final QueueConsumer queueConsumer2 = queueRunnableFactory.createQueueListener("test-queue", cmc2);
        final QueueProducer<Object> queueProducer = queueRunnableFactory.createQueueJsonMessageProducer("test-queue");
        final Thread consumerThread1 = new Thread(queueConsumer1);
        final Thread consumerThread2 = new Thread(queueConsumer2);
        final Thread producerThread = new Thread(queueProducer);
        consumerThread1.start();
        consumerThread2.start();
        producerThread.start();

        Thread.sleep(1000L);

        Assert.assertTrue(queueConsumer1.isConnected());
        Assert.assertTrue(queueConsumer2.isConnected());
        Assert.assertFalse(queueProducer.isConnected());

        final int maxCount = 1000;
        for (int i = 0; i < maxCount; i++) {
            queueProducer.put(format("hello, world %d", i));
        }

        for (int i = 0; i < 100 && !queueProducer.isEmpty(); i++) {
            Thread.sleep(10L);
        }
        Assert.assertTrue(queueProducer.isEmpty());
        Assert.assertEquals(maxCount, cmc1.getCount() + cmc2.getCount());

        queueProducer.shutdown();
        queueConsumer1.shutdown();
        queueConsumer2.shutdown();
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
        final QueueConsumer queueConsumer = queueRunnableFactory.createQueueListener("test-queue", cmc);
        final QueueProducer<Object> queueProducer1 = queueRunnableFactory.createQueueJsonMessageProducer("test-queue");
        final QueueProducer<Object> queueProducer2 = queueRunnableFactory.createQueueJsonMessageProducer("test-queue");
        final Thread consumerThread = new Thread(queueConsumer);
        final Thread producerThread1 = new Thread(queueProducer1);
        final Thread producerThread2 = new Thread(queueProducer2);

        consumerThread.start();
        producerThread1.start();
        producerThread2.start();

        Thread.sleep(1000L);

        Assert.assertTrue(queueConsumer.isConnected());
        Assert.assertFalse(queueProducer1.isConnected());
        Assert.assertFalse(queueProducer2.isConnected());

        final int maxCount = 1000;
        for (int i = 0; i < maxCount; i++) {
            queueProducer1.put(format("hello, world %d", i));
            queueProducer2.put(format("hello, wold %d", i));
        }

        for (int i = 0; i < 100 && !queueProducer1.isEmpty() && !queueProducer2.isEmpty(); i++) {
            Thread.sleep(10L);
        }
        Assert.assertTrue(queueProducer1.isEmpty());
        Assert.assertTrue(queueProducer2.isEmpty());
        Assert.assertEquals(maxCount*2, cmc.getCount());

        queueProducer1.shutdown();
        queueProducer2.shutdown();
        queueConsumer.shutdown();
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
        final QueueConsumer queueConsumer1 = queueRunnableFactory.createQueueListener("test-queue", cmc1);
        final QueueConsumer queueConsumer2 = queueRunnableFactory.createQueueListener("test-queue", cmc2);
        final QueueProducer<Object> queueProducer1 = queueRunnableFactory.createQueueJsonMessageProducer("test-queue");
        final QueueProducer<Object> queueProducer2 = queueRunnableFactory.createQueueJsonMessageProducer("test-queue");
        final Thread consumerThread1 = new Thread(queueConsumer1);
        final Thread consumerThread2 = new Thread(queueConsumer2);
        final Thread producerThread1 = new Thread(queueProducer1);
        final Thread producerThread2 = new Thread(queueProducer2);

        consumerThread1.start();
        consumerThread2.start();
        producerThread1.start();
        producerThread2.start();

        Thread.sleep(1000L);

        Assert.assertTrue(queueConsumer1.isConnected());
        Assert.assertTrue(queueConsumer2.isConnected());
        Assert.assertFalse(queueProducer1.isConnected());
        Assert.assertFalse(queueProducer2.isConnected());

        final int maxCount = 1000;
        for (int i = 0; i < maxCount; i++) {
            queueProducer1.put(format("hello, world %d", i));
            queueProducer2.put(format("hello, wold %d", i));
        }

        for (int i = 0; i < 100 && !queueProducer1.isEmpty() && !queueProducer2.isEmpty(); i++) {
            Thread.sleep(10L);
        }

        Assert.assertTrue(queueProducer1.isEmpty());
        Assert.assertTrue(queueProducer2.isEmpty());

        Assert.assertEquals(maxCount*2, cmc1.getCount() + cmc2.getCount());

        queueProducer1.shutdown();
        queueProducer2.shutdown();
        queueConsumer1.shutdown();
        queueConsumer2.shutdown();
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



