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

import javax.annotation.Nullable;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

/**
 * Accepts incoming messages, checks whether they are text messages and forwards them to
 * a message callback.
 */
public final class TextMessageConsumerCallback implements ConsumerCallback<Message>
{
    private static final Log LOG = Log.findLog();

    private final ConsumerCallback<String> messageCallback;


    TextMessageConsumerCallback(@Nullable final ConsumerCallback<String> messageCallback)
    {
        this.messageCallback = messageCallback;
    }

    @Override
    public boolean withMessage(final Message message) throws IOException, JMSException
    {
        if (message == null) {
            return true;
        }
        else if (message instanceof TextMessage) {
            final String text = ((TextMessage) message).getText();
            if (messageCallback != null) {
                return messageCallback.withMessage(text);
            }
        }
        else {
            LOG.warn("Non-text message received (%s)!",message);
        }

        return true;
    }
}
