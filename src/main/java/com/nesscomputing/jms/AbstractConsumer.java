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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;

/**
 * A general runnable that will keep the connection to a queue or topic alive and dispatch messages
 * received to an instance that implements {@link ConsumerCallback}.
 */
public abstract class AbstractConsumer extends AbstractJmsRunnable
{
    private AtomicReference<MessageConsumer> consumerHolder = new AtomicReference<MessageConsumer>();

    private final ConsumerCallback<Message> messageCallback;
    private final long tickTimeout;

    protected AbstractConsumer(@Nonnull final ConnectionFactory connectionFactory,
                               @Nonnull final JmsConfig jmsConfig,
                               @Nonnull final String topic,
                               @Nonnull final ConsumerCallback<Message> messageCallback)
    {
        super(connectionFactory, jmsConfig, topic);

        this.messageCallback = messageCallback;
        this.tickTimeout = getConfig().getTickTimeout().getMillis();

    }

    @Override
    protected void sessionDisconnect()
    {
        final MessageConsumer consumer = consumerHolder.getAndSet(null);
        JmsUtils.closeQuietly(consumer);

        super.sessionDisconnect();
    }

    @Override
    protected boolean process() throws JMSException, IOException
    {
        sessionConnect();
        final MessageConsumer consumer = consumerHolder.get();

        final Message message;
        if (consumer != null) {
            message = consumer.receive(tickTimeout);
        }
        else {
            LOG.warn("Session connected but no consumer available!");
            message = null;
            try {
                Thread.sleep(tickTimeout);
            }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        if (message == null) {
            LOG.trace("Tick...");
        }

        return messageCallback.withMessage(message);
    }

    protected void setConsumer(final MessageConsumer consumer)
    {
        this.consumerHolder.set(consumer);
    }
}
