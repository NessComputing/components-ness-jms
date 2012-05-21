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

import javax.annotation.CheckForNull;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

/**
 * Converts Strings into text messages.
 */
public final class TextMessageProducerCallback implements ProducerCallback<String>
{
    @Override
    @CheckForNull
    public Message buildMessage(final AbstractProducer<String> producer, final String data) throws IOException, JMSException
    {
        final TextMessage message = producer.createTextMessage(data);
        if (message == null) {
            throw new JMSException("Could not create text message, not connected?");
        }
        return message;
    }
}
