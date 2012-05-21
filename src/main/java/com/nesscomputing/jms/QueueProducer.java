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

import javax.annotation.Nonnull;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;

/**
 * A general runnable that will keep the connection to a queue alive and enqueue messages
 * on the queue.
 */
public final class QueueProducer<T> extends AbstractProducer<T>
{
    public QueueProducer(@Nonnull final ConnectionFactory connectionFactory,
                         @Nonnull final JmsConfig jmsConfig,
                         @Nonnull final String topic,
                         @Nonnull final ProducerCallback<T> producerCallback)

    {
        super(connectionFactory, jmsConfig, topic, producerCallback);
    }

    @Override
    protected String getServiceType()
    {
        return "queue-producer";
    }

    @Override
    protected void connectCallback(final Session session) throws JMSException
    {
        setProducer(session.createProducer(session.createQueue(getName())));
    }
}
