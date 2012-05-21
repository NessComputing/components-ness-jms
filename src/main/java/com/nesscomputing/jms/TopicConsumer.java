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
import javax.jms.Message;
import javax.jms.Session;

import com.google.inject.Inject;

/**
 * A general runnable that will keep the connection to a topic alive and dispatch messages
 * received on the topic to an instance that implements {@link ConsumerCallback}.
 */
public final class TopicConsumer extends AbstractConsumer
{
    @Inject
    public TopicConsumer(@Nonnull final ConnectionFactory connectionFactory,
                         @Nonnull final JmsConfig jmsConfig,
                         @Nonnull final String topic,
                         @Nonnull final ConsumerCallback<Message> messageCallback)
    {
        super(connectionFactory, jmsConfig, topic, messageCallback);
    }

    @Override
    protected String getServiceType()
    {
        return "topic-consumer";
    }

    @Override
    protected void connectCallback(final Session session) throws JMSException
    {
        setConsumer(session.createConsumer(session.createTopic(getName())));
    }
}
