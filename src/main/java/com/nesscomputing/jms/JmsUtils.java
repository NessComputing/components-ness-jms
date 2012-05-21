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

import java.io.InterruptedIOException;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

/**
 * Helper methods to deal with JMS stuff.
 */
public final class JmsUtils
{
    private static final Log LOG = Log.findLog();

    private JmsUtils()
    {
    }

    /**
     * Close a message consumer.
     *
     * @param consumer
     */
    public static void closeQuietly(final MessageConsumer consumer)
    {
        if (consumer != null) {
            try {
                consumer.close();
            }
            catch (JMSException je) {
                if (je.getCause() instanceof InterruptedException) {
                    LOG.trace("ActiveMQ caught and wrapped InterruptedException");
                }
                if (je.getCause() instanceof InterruptedIOException) {
                    LOG.trace("ActiveMQ caught and wrapped InterruptedIOException");
                }
                else {
                    LOG.warnDebug(je, "While closing consumer");
                }
            }
        }
    }

    /**
     * Close a message producer.
     *
     * @param producer
     */
    public static void closeQuietly(final MessageProducer producer)
    {
        if (producer != null) {
            try {
                producer.close();
            }
            catch (JMSException je) {
                if (je.getCause() instanceof InterruptedException) {
                    LOG.trace("ActiveMQ caught and wrapped InterruptedException");
                }
                if (je.getCause() instanceof InterruptedIOException) {
                    LOG.trace("ActiveMQ caught and wrapped InterruptedIOException");
                }
                else {
                    LOG.warnDebug(je, "While closing producer");
                }
            }
        }
    }

    /**
     * Close a session.
     *
     * @param session
     */
    public static void closeQuietly(final Session session)
    {
        if (session != null) {
            try {
                session.close();
            }
            catch (JMSException je) {
                if (je.getCause() instanceof InterruptedException) {
                    LOG.trace("ActiveMQ caught and wrapped InterruptedException");
                }
                if (je.getCause() instanceof InterruptedIOException) {
                    LOG.trace("ActiveMQ caught and wrapped InterruptedIOException");
                }
                else {
                    LOG.warnDebug(je, "While closing session");
                }
            }
        }
    }

    /**
     * Close a connection.
     *
     * @param connection
     */
    public static void closeQuietly(final Connection connection)
    {
        if (connection != null) {
            try {
                connection.close();
            }
            catch (JMSException je) {
                if (je.getCause() instanceof InterruptedException) {
                    LOG.trace("ActiveMQ caught and wrapped InterruptedException");
                }
                if (je.getCause() instanceof InterruptedIOException) {
                    LOG.trace("ActiveMQ caught and wrapped InterruptedIOException");
                }
                else {
                    LOG.warnDebug(je, "While closing connection");
                }
            }
        }
    }
}
