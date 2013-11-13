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

import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.Message;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.google.inject.name.Named;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.nesscomputing.config.Config;
import com.nesscomputing.config.ConfigModule;

public class TestStrangeStuff
{
    @Inject
    @Named("test")
    public JmsRunnableFactory topicRunnableFactory;

    @Before
    public void setUp()
    {
        final Config config = Config.getFixedConfig(ImmutableMap.of("ness.jms.test.enabled", "true",
                                                                                          "ness.jms.test.connection-url", "vm://testbroker?broker.persistent=false"));
        final Injector injector = Guice.createInjector(Stage.PRODUCTION,
                                                       new ConfigModule(config),
                                                       new JmsModule(config, "test"));

        injector.injectMembers(this);

        Assert.assertNotNull(topicRunnableFactory);
    }

    @Test
    public void testConsumerDies() throws Exception
    {
        final ConsumerCallback<Message> callback = new ConsumerCallback<Message>() {

            @Override
            public boolean withMessage(Message message) throws JMSException {
                return false;
            }
        };

        final TopicConsumer topicConsumer = topicRunnableFactory.createTopicListener("test-topic", callback);
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
            topicProducer.put("dummy");
        }

        for (int i = 0; i < 100 && !topicProducer.isEmpty(); i++) {
            Thread.sleep(10L);
        }
        Thread.sleep(100L);

        Assert.assertTrue(topicProducer.isEmpty());
        Assert.assertTrue(topicProducer.isConnected());
        Assert.assertFalse(topicConsumer.isConnected());

        topicProducer.shutdown();
        topicConsumer.shutdown();
        producerThread.interrupt();
        consumerThread.interrupt();
        producerThread.join();
        consumerThread.join();
    }

    @Test
    public void testConsumerGetsInterrupted() throws Exception
    {
        final ConsumerCallback<Message> callback = new ConsumerCallback<Message>() {

            @Override
            public boolean withMessage(Message message) throws JMSException {
                return true;
            }
        };

        final TopicConsumer topicConsumer = topicRunnableFactory.createTopicListener("test-topic", callback);
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
            topicProducer.put("dummy");
            if (i > 500) {
                consumerThread.interrupt();
            }
        }

        for (int i = 0; i < 100 && !topicProducer.isEmpty(); i++) {
            Thread.sleep(10L);
        }
        Thread.sleep(100L);

        Assert.assertTrue(topicProducer.isEmpty());
        Assert.assertTrue(topicProducer.isConnected());
        Assert.assertFalse(topicConsumer.isConnected());

        topicProducer.shutdown();
        topicConsumer.shutdown();
        producerThread.interrupt();
        consumerThread.interrupt();
        producerThread.join();
        consumerThread.join();
    }

    @Test
    public void testProducerGetsInterrupted() throws Exception
    {
        final ConsumerCallback<Message> callback = new ConsumerCallback<Message>() {

            @Override
            public boolean withMessage(Message message) throws JMSException {
                return true;
            }
        };

        final TopicConsumer topicConsumer = topicRunnableFactory.createTopicListener("test-topic", callback);
        final TopicProducer<Object> topicProducer = topicRunnableFactory.createTopicJsonProducer("test-topic");
        final Thread consumerThread = new Thread(topicConsumer);
        final Thread producerThread = new Thread(topicProducer);
        consumerThread.start();
        producerThread.start();

        Thread.sleep(1000L);

        Assert.assertTrue(topicConsumer.isConnected());
        Assert.assertFalse(topicProducer.isConnected());

        final int maxCount = 10000;
        int i = 0;

        for (i = 0; i < maxCount; i++) {
            if (!topicProducer.offerWithTimeout("dummy", 50L, TimeUnit.MILLISECONDS)) {
                break;
            }

            if (i > 500) {
                producerThread.interrupt();
            }
        }

        Assert.assertFalse(i == maxCount);
        Assert.assertTrue(i > 500);

        Thread.sleep(500L);
        Assert.assertFalse(topicProducer.isConnected());
        Assert.assertTrue(topicConsumer.isConnected());

        topicProducer.shutdown();
        topicConsumer.shutdown();
        producerThread.interrupt();
        consumerThread.interrupt();
        producerThread.join();
        consumerThread.join();
    }
}

