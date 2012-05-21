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
package com.nesscomputing.jms.util;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Message;

import com.nesscomputing.jms.ConsumerCallback;


public class CountingMessageCallback implements ConsumerCallback<Message>
{
    private AtomicInteger counter = new AtomicInteger(0);

    @Override
    public boolean withMessage(final Message message)
    {
        if (message != null) {
            counter.incrementAndGet();
        }
        return true;
    }

    public int getCount()
    {
        return counter.get();
    }
}
