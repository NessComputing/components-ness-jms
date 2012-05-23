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

import org.skife.config.Config;
import org.skife.config.Default;
import org.skife.config.DefaultNull;
import org.skife.config.TimeSpan;

/**
 * Hold configuration parameters for the JMS infrastructure.
 *
 * This class <b>is not a part of the public API</b> and must not be used
 * outside of ness-jms.  It is public so that subpackages may access it.
 */
public abstract class JmsConfig
{
    /**
     * Enable or disable a JMS connection.
     */
    @Config({"ness.jms.${name}.enabled", "ness.jms.enabled"})
    @Default("true")
    public boolean isEnabled()
    {
        return true;
    }

    /**
     * Whether the transmitter connects immediately or lazy.
     */
    @Config({"ness.jms.${name}.lazy-transmitter-connect", "ness.jms.lazy-transmitter-connect"})
    @Default("true")
    public boolean isLazyTransmitterConnect()
    {
        return true;
    }

    /**
     * Connection URL for JMS.  Is interpreted as a format string where the first %s is used
     * as a uniquifier for discovery transports.  This marker is optional for the case where
     * you do not use service discovery.  Example: <code>srvc://activemq?discoveryId=%s</code>
     */
    @Config({"ness.jms.${name}.connection-url", "ness.jms.connection-url"})
    @DefaultNull
    public String getJmsConnectionUrlFormat()
    {
        return null;
    }

    /**
     * If the topic is connected, the message callback will be called after this many millis, no matter whether a message was
     * received or not.
     */
    @Config({"ness.jms.${name}.tick-timeout", "ness.jms.tick-timeout"})
    @Default("5s")
    public TimeSpan getTickTimeout()
    {
        return new TimeSpan("5s");
    }

    /**
     * If a problem occurs, the backoff delay calculated with this delay.
     */
    @Config({"ness.jms.${name}.backoff-delay", "ness.jms.backoff-delay"})
    @Default("3s")
    public TimeSpan getBackoffDelay()
    {
        return new TimeSpan("3s");
    }

    /**
     * maximum shift factor (2^1 .. 2^x) for the exponential backoff.
     */
    @Config({"ness.jms.${name}.max-backoff-factor", "ness.jms.max-backoff-factor"})
    @Default("6")
    public int getMaxBackoffFactor()
    {
        return 6;
    }

    /**
     * Internal queue length for the producer thread.
     */
    @Config({"ness.jms.${name}.producer-queue-length", "ness.jms.producer-queue-length"})
    @Default("20")
    public int getProducerQueueLength()
    {
        return 20;
    }


    /**
     * Maximum amount of time that the transmitter tries to
     * enqueue an event onto the topic or queue before it
     * gives up and drops the event.
     */
    @Config({"ness.jms.${name}.transmit-timeout", "ness.jms.transmit-timeout"})
    @Default("10ms")
    public TimeSpan getTransmitTimeout()
    {
        return new TimeSpan("10ms");
    }

    /**
     * Cooloff time after failing to enqueue an event.
     */
    @Config({"ness.jms.${name}.failure-cooloff-time", "ness.jms.failure-cooloff-time"})
    @Default("1s")
    public TimeSpan getFailureCooloffTime()
    {
        return new TimeSpan("1s");
    }
}
