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

import java.lang.annotation.Annotation;

import javax.annotation.Nullable;
import javax.jms.ConnectionFactory;
import javax.jms.Message;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Singleton;

/**
 * Factory to create new Runnables to access topics and queues.
 */
@Singleton
public class JmsRunnableFactory
{
    private final Annotation annotation;

    private ConnectionFactory connectionFactory;
    private JmsConfig jmsConfig;
    private JsonProducerCallback producerCallback;

    JmsRunnableFactory(@Nullable final Annotation annotation)
    {
        this.annotation = annotation;
    }

    @Inject
    void inject(final Injector injector, final JsonProducerCallback jsonProducerCallback)
    {
        if (annotation == null) {
            this.connectionFactory = injector.getInstance(Key.get(ConnectionFactory.class));
            this.jmsConfig = injector.getInstance(Key.get(JmsConfig.class));
        }
        else {
            this.connectionFactory = injector.getInstance(Key.get(ConnectionFactory.class, annotation));
            this.jmsConfig = injector.getInstance(Key.get(JmsConfig.class, annotation));
        }

        this.producerCallback = jsonProducerCallback;
    }

    /**
     * Creates a new {@link TopicProducer}. The callback is called to convert an object that is sent into the TopicProducer
     * into a JMS message type.
     */
    public <T> TopicProducer<T> createTopicProducer(final String topic, final ProducerCallback<T> messageCallback)
    {
        Preconditions.checkState(connectionFactory != null, "connection factory was never injected!");
        return new TopicProducer<T>(connectionFactory, jmsConfig, topic, messageCallback);
    }

    /**
     * Creates a new {@link QueueProducer}. The callback is called to convert an object that is sent into the QueueProducer
     * into a JMS message type.
     */
    public <T> QueueProducer<T> createQueueProducer(final String topic, final ProducerCallback<T> messageCallback)
    {
        Preconditions.checkState(connectionFactory != null, "connection factory was never injected!");
        return new QueueProducer<T>(connectionFactory, jmsConfig, topic, messageCallback);
    }

    /**
     * Creates a new {@link TopicProducer}. The producer accepts arbitrary objects and uses the Jackson object mapper to convert them into
     * JSON and sends them as a text message.
     */
    public TopicProducer<Object> createTopicJsonProducer(final String topic)
    {
        Preconditions.checkState(connectionFactory != null, "connection factory was never injected!");
        return new TopicProducer<Object>(connectionFactory, jmsConfig, topic, producerCallback);
    }

    /**
     * Creates a new {@link QueueProducer}. The producer accepts arbitrary objects and uses the Jackson object mapper to convert them into
     * JSON and sends them as a text message.
     */
    public QueueProducer<Object> createQueueJsonMessageProducer(final String topic)
    {
        Preconditions.checkState(connectionFactory != null, "connection factory was never injected!");
        return new QueueProducer<Object>(connectionFactory, jmsConfig, topic, producerCallback);
    }

    /**
     * Creates a new {@link TopicProducer}. The producer accepts strings and sends them as a text message.
     */
    public TopicProducer<String> createTopicTextMessageProducer(final String topic)
    {
        Preconditions.checkState(connectionFactory != null, "connection factory was never injected!");
        return new TopicProducer<String>(connectionFactory, jmsConfig, topic, new TextMessageProducerCallback());
    }

    /**
     * Creates a new {@link QueueProducer}. The producer accepts strings and sends them as a text message.
     */
    public QueueProducer<String> createQueueTextMessageProducer(final String topic)
    {
        Preconditions.checkState(connectionFactory != null, "connection factory was never injected!");
        return new QueueProducer<String>(connectionFactory, jmsConfig, topic, new TextMessageProducerCallback());
    }

    /**
     * Creates a new {@link TopicConsumer}. For every message received (or when the timeout waiting for messages is hit), the callback
     * is invoked with the message received.
     */
    public TopicConsumer createTopicListener(final String topic, final ConsumerCallback<Message> messageCallback)
    {
        Preconditions.checkState(connectionFactory != null, "connection factory was never injected!");
        return new TopicConsumer(connectionFactory, jmsConfig, topic, messageCallback);
    }

    /**
     * Creates a new {@link QueueConsumer}. For every message received (or when the timeout waiting for messages is hit), the callback
     * is invoked with the message received.
     */
    public QueueConsumer createQueueListener(final String topic, final ConsumerCallback<Message> messageCallback)
    {
        Preconditions.checkState(connectionFactory != null, "connection factory was never injected!");
        return new QueueConsumer(connectionFactory, jmsConfig, topic, messageCallback);
    }

    /**
     * Creates a new {@link TopicConsumer}. For every text message received, the callback
     * is invoked with the contents of the text message as string.
     */
    public TopicConsumer createTopicTextMessageListener(final String topic, final ConsumerCallback<String> messageCallback)
    {
        Preconditions.checkState(connectionFactory != null, "connection factory was never injected!");
        return new TopicConsumer(connectionFactory, jmsConfig, topic, new TextMessageConsumerCallback(messageCallback));
    }

    /**
     * Creates a new {@link QueueConsumer}. For every text message received, the callback
     * is invoked with the contents of the text message as string.
     */
    public QueueConsumer createQueueTextMessageListener(final String topic, final ConsumerCallback<String> messageCallback)
    {
        Preconditions.checkState(connectionFactory != null, "connection factory was never injected!");
        return new QueueConsumer(connectionFactory, jmsConfig, topic, new TextMessageConsumerCallback(messageCallback));
    }

}
