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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

import com.google.common.base.Preconditions;

import com.nesscomputing.logging.Log;

/**
 * A general runnable that will keep the connection to a topic or queue alive and enqueue messages.
 */
public abstract class AbstractProducer<T> extends AbstractJmsRunnable
{
    private static final Log LOG = Log.findLog();

    private MessageProducer producer = null;

    private final BlockingQueue<T> messageQueue;
    private final ProducerCallback<T> producerCallback;
    private final AtomicBoolean initialSleepSkipped = new AtomicBoolean();

    protected AbstractProducer(@Nonnull final ConnectionFactory connectionFactory,
                               @Nonnull final JmsConfig jmsConfig,
                               @Nonnull final String topic,
                               @Nonnull final ProducerCallback<T> producerCallback)

    {
        super(connectionFactory, jmsConfig, topic);
        this.producerCallback = producerCallback;

        this.messageQueue = new ArrayBlockingQueue<T>(getConfig().getProducerQueueLength());
    }

    protected void setProducer(final MessageProducer producer)
    {
        this.producer = producer;
    }

    public boolean offer(@Nonnull final T data)
    {
        Preconditions.checkArgument(data != null, "the message can not be null!");
        return messageQueue.offer(data);
    }

    public boolean offerWithTimeout(@Nonnull final T data)
    {
        Preconditions.checkArgument(data != null, "the message can not be null!");
        boolean success = false;

        try {
            success = messageQueue.offer(data, getConfig().getTransmitTimeout().getPeriod(), getConfig().getTransmitTimeout().getUnit());
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return success;
    }

    public boolean offerWithTimeout(@Nonnull final T data, final long timeout, final TimeUnit unit) throws InterruptedException
    {
        Preconditions.checkArgument(data != null, "the message can not be null!");
        return messageQueue.offer(data, timeout, unit);
    }

    public void put(@Nonnull final T data)
    {
        Preconditions.checkArgument(data != null, "the message can not be null!");
        try {
            messageQueue.put(data);
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    public int remainingCapacity()
    {
        return messageQueue.remainingCapacity();
    }

    public int size()
    {
        return messageQueue.size();
    }

    public boolean isEmpty()
    {
        return messageQueue.isEmpty();
    }

    @Override
    protected void sessionDisconnect()
    {
        JmsUtils.closeQuietly(producer);
        this.producer = null;

        super.sessionDisconnect();
    }

    @Override
    protected boolean process() throws JMSException, IOException, InterruptedException
    {
        // Only connect the transmitter if lazy-connect is false.
        if (!getConfig().isLazyTransmitterConnect()) {
            sessionConnect();
        }

        // This outer loop only happens once at startup and then again every time we bail due to an error
        if (initialSleepSkipped.compareAndSet(false, true)) {
            Thread.sleep(getConfig().getFailureCooloffTime().getMillis());
        }

        while (true) {
            T data = messageQueue.take();
            final Message message = producerCallback.buildMessage(this, data);
            long startNanos = System.nanoTime();
            if (message == null) {
                LOG.warn("Dropping message '%s' because %s failed to build a JMS Message.", data, producerCallback);
                // In cooloff timeout.
                break;
            }
            producer.send(message);
            LOG.trace("Sent message '%s' in %sus, queue size now %s", message, TimeUnit.MICROSECONDS.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS), size());
        }
        return true;
    }
}
