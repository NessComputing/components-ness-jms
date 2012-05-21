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
import javax.annotation.Nullable;
import javax.jms.JMSException;
import javax.jms.Message;

public interface ProducerCallback<T>
{
    /**
     * Callback for every object to transmit.
     *
     * @param data The object to transmit.
     * @return A message object to transmit or null.
     */
    @CheckForNull
    Message buildMessage(@Nonnull AbstractProducer<T> producer, @Nullable T data) throws JMSException, IOException;
}

