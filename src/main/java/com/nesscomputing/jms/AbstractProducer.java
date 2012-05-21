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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

import com.google.common.base.Preconditions;

/**
 * A general runnable that will keep the connection to a topic or queue alive and enqueue messages.
 */
public abstract class AbstractProducer<T> extends AbstractJmsRunnable
{
    private MessageProducer producer = null;

    private final AtomicLong cooloffTime = new AtomicLong(-1L);

    private final BlockingQueue<T> messageQueue;
    private final ProducerCallback<T> producerCallback;
    private final Semaphore queueSemaphore = new Semaphore(0);

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

        final boolean success = messageQueue.offer(data);

        if (success) {
            queueSemaphore.release(1);
        }
        else {
            updateCooloff();
        }
        return success;
    }

    public boolean offerWithTimeout(@Nonnull final T data)
    {
        Preconditions.checkArgument(data != null, "the message can not be null!");
        boolean success = false;

        try {
            success = messageQueue.offer(data, getConfig().getTransmitTimeout().getPeriod(), getConfig().getTransmitTimeout().getUnit());

            if (success) {
                queueSemaphore.release(1);
            }
            else {
                updateCooloff();
            }
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            updateCooloff();
        }
        return success;
    }

    public boolean offerWithTimeout(@Nonnull final T data, final long timeout, final TimeUnit unit) throws InterruptedException
    {
        Preconditions.checkArgument(data != null, "the message can not be null!");
        boolean success = false;

        try {
            success = messageQueue.offer(data, timeout, unit);

            if (success) {
                queueSemaphore.release(1);
            }
            else {
                updateCooloff();
            }
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            updateCooloff();
        }
        return success;
    }

    public void put(@Nonnull final T data)
    {
        Preconditions.checkArgument(data != null, "the message can not be null!");
        try {
            messageQueue.put(data);
            queueSemaphore.release(1);
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            updateCooloff();
        }
    }

    public int remainingCapacity()
    {
        return messageQueue.remainingCapacity();
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

        boolean permit = queueSemaphore.tryAcquire(getConfig().getTickTimeout().getPeriod(), getConfig().getTickTimeout().getUnit());

        T data = null;
        try {
            while ((data = messageQueue.peek()) != null) {
                // Remove a permit for the object we process. It is actually possible that
                // two permits are pulled for one object (one above and one now), but that does
                // not matter, because this look will drain the queue anyway and if there are permits
                // left if the queue is empty, all that happens are a couple of spurious wakeups (the tryAcquire with
                // timeout will consume them). It should not be possible to leave this loop and have no permits and
                // still objects in the queue (because permits are only removed when objects exist).
                if (!permit) {
                    permit = queueSemaphore.tryAcquire();
                }
                final Message message = producerCallback.buildMessage(this, data);
                if (message == null) {
                    // In cooloff timeout.
                    break;
                }
                producer.send(message);
                messageQueue.poll();
                permit = false;
            }
            clearCooloff();
        }
        catch (IOException ioe) {
            updateCooloff();
            throw ioe;
        }
        catch (JMSException je) {
            updateCooloff();
            throw je;
        }
        return true;
    }

    private void updateCooloff()
    {
        this.cooloffTime.compareAndSet(-1L, System.nanoTime() + getConfig().getFailureCooloffTime().getMillis() * 1000000L);
    }

    private void clearCooloff()
    {
        this.cooloffTime.set(-1L);
    }
}
