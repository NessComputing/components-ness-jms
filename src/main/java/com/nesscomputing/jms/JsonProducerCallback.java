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
import javax.annotation.Nonnull;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;

/**
 * Converts arbitrary objects into JSON and returns a text message containing it.
 */
public final class JsonProducerCallback implements ProducerCallback<Object>
{
    private ObjectMapper mapper = null;

    @Inject(optional=true)
    void injectMapper(@Nonnull final ObjectMapper mapper)
    {
        this.mapper = mapper;
    }

    @Override
    @CheckForNull
    public Message buildMessage(final AbstractProducer<Object> producer, final Object data) throws IOException, JMSException
    {
        Preconditions.checkState(mapper != null, "need object mapper configured!");

        final TextMessage message = producer.createTextMessage();
        if (message == null) {
            throw new JMSException("Could not create text message, not connected?");
        }
        else {
            final String dataText = mapper.writeValueAsString(data);
            message.setText(dataText);
            return message;
        }
    }
}
