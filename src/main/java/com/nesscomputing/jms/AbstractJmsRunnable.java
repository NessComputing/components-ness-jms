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

import com.nesscomputing.logging.Log;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import com.google.common.base.Preconditions;

/**
 * Base code for the topic and queue runnables.
 */
public abstract class AbstractJmsRunnable implements Runnable
{
    protected final Log LOG = Log.forClass(this.getClass());

    private AtomicReference<Connection> connectionHolder = new AtomicReference<Connection>();
    private AtomicReference<Session> sessionHolder = new AtomicReference<Session>();
    private AtomicBoolean running = new AtomicBoolean(true);
    private AtomicBoolean connected = new AtomicBoolean(false);

    private int backoff = 1;

    private final ConnectionFactory connectionFactory;
    private final JmsConfig jmsConfig;
    private final String name;

    protected AbstractJmsRunnable(@Nonnull final ConnectionFactory connectionFactory,
                                  @Nonnull final JmsConfig jmsConfig,
                                  @Nonnull final String name)
    {
        Preconditions.checkState(name != null, "The name can not be null!");
        this.connectionFactory = connectionFactory;
        this.jmsConfig = jmsConfig;
        this.name = name;
    }

    public void shutdown()
    {
        running.set(false);
    }

    public boolean isRunning()
    {
        return running.get();
    }

    public boolean isConnected()
    {
        return connected.get();
    }

    @Override
    public void run()
    {
        LOG.debug("Starting %s for '%s'", getServiceType(), name);
        try {
            while (running.get()) {
                try {
                    if (!process()) {
                        break; // while
                    }

                }
                catch (JMSException je) {
                    if (je.getCause() instanceof InterruptedException) {
                        LOG.trace("ActiveMQ caught and wrapped InterruptedException");
                        throw InterruptedException.class.cast(je.getCause());
                    }
                    if (je.getCause() instanceof InterruptedIOException) {
                        LOG.trace("ActiveMQ caught and wrapped InterruptedIOException");
                        running.set(false);
                        break;
                    }
                    else {
                        backoff(je.getCause());
                    }
                }
                catch (IOException ioe) {
                    backoff(ioe);
                }
                // Catch all exceptions here, not just IOException. This makes sure that
                // with a catastrophic failure in the processor, the thread does not die.
                catch (RuntimeException re) {
                    LOG.warnDebug(re, "Caught an exception in time before!");
                    backoff(re);
                }
            }
        }
        catch (InterruptedException ie) {
            running.set(false);
            LOG.trace("Terminated by interrupt");
        }
        finally {
            LOG.debug("Stopping %s for '%s'", getServiceType(), name);
            sessionDisconnect();
        }
    }

    private void backoff(final Throwable t) throws InterruptedException
    {
        final long backoffTime = jmsConfig.getBackoffDelay().getMillis() * backoff;
        LOG.warnDebug(t, "Could not connect to Broker, sleeping for %d ms...", backoffTime);

        Thread.sleep(backoffTime);
        if (backoff != 1 << jmsConfig.getMaxBackoffFactor()) {
            backoff <<= 1;
        }
        sessionDisconnect();
    }

    protected final String getName()
    {
        return name;
    }

    protected final JmsConfig getConfig()
    {
        return jmsConfig;
    }


    protected abstract String getServiceType();

    protected abstract void connectCallback(final Session session) throws JMSException;

    protected abstract boolean process() throws JMSException, InterruptedException, IOException;

    protected void sessionDisconnect()
    {
        final Session session = sessionHolder.getAndSet(null);
        JmsUtils.closeQuietly(session);

        final Connection connection = connectionHolder.getAndSet(null);
        JmsUtils.closeQuietly(connection);

        this.connected.set(false);
    }

    protected Session sessionConnect() throws JMSException
    {
        if (connected.get()) {
            return sessionHolder.get();
        }
        else {
            final Connection connection = connectionFactory.createConnection();
            connectionHolder.set(connection);
            connection.start();

            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            sessionHolder.set(session);

            connectCallback(session);

            connected.set(true);
            backoff = 1;
            return session;
        }
    }

    @CheckForNull
    public BytesMessage createBytesMessage() throws JMSException
    {
        return withSession(new SessionCallback<BytesMessage>() {
            @Override
            public BytesMessage withSession(final Session session) throws JMSException {
                return session.createBytesMessage();
            }
        });
    }

    @CheckForNull
    public MapMessage createMapMessage() throws JMSException
    {
        return withSession(new SessionCallback<MapMessage>() {
            @Override
            public MapMessage withSession(final Session session) throws JMSException {
                return session.createMapMessage();
            }
        });
    }

    @CheckForNull
    public Message createMessage() throws JMSException
    {
        return withSession(new SessionCallback<Message>() {
            @Override
            public Message withSession(final Session session) throws JMSException {
                return session.createMessage();
            }
        });
    }

    @CheckForNull
    public ObjectMessage createObjectMessage() throws JMSException
    {
        return withSession(new SessionCallback<ObjectMessage>() {
            @Override
            public ObjectMessage withSession(final Session session) throws JMSException {
                return session.createObjectMessage();
            }
        });
    }

    @CheckForNull
    public ObjectMessage createObjectMessage(final Serializable object) throws JMSException
    {
        return withSession(new SessionCallback<ObjectMessage>() {
            @Override
            public ObjectMessage withSession(final Session session) throws JMSException {
                return session.createObjectMessage(object);
            }
        });
    }

    @CheckForNull
    public StreamMessage createStreamMessage() throws JMSException
    {
        return withSession(new SessionCallback<StreamMessage>() {
            @Override
            public StreamMessage withSession(final Session session) throws JMSException {
                return session.createStreamMessage();
            }
        });
    }

    @CheckForNull
    public TextMessage createTextMessage() throws JMSException
    {
        return withSession(new SessionCallback<TextMessage>() {
            @Override
            public TextMessage withSession(final Session session) throws JMSException {
                return session.createTextMessage();
            }
        });
    }

    @CheckForNull
    public TextMessage createTextMessage(final String text) throws JMSException
    {
        return withSession(new SessionCallback<TextMessage>() {
            @Override
            public TextMessage withSession(final Session session) throws JMSException {
                return session.createTextMessage(text);
            }
        });
    }

    private <T> T withSession(SessionCallback<T> callback) throws JMSException {
        final Session session = sessionConnect();
        if (session != null) {
            try {
                return callback.withSession(session);
            }
            catch (JMSException je) {
                sessionDisconnect();
                throw je;
            }
        }
        return null;
    }

    public interface SessionCallback<T>
    {
        T withSession(Session session) throws JMSException;
    }

}
