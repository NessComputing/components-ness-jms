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

import static java.lang.String.format;

import java.lang.annotation.Annotation;
import java.net.URI;
import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.ProvisionException;
import com.google.inject.TypeLiteral;

/**
 * Provides a Connection factory to retrieve JMS connections from ActiveMQ.
 */
final class ActiveMQConnectionFactoryProvider implements Provider<ConnectionFactory>
{
    private final JmsConfig jmsConfig;
    private final Annotation annotation;
    private Set<JmsUriInterceptor> interceptors = Collections.emptySet();

    ActiveMQConnectionFactoryProvider(@Nonnull final JmsConfig jmsConfig, @Nonnull final Annotation annotation)
    {
        Preconditions.checkArgument(annotation != null, "no binding annotation");

        this.jmsConfig = jmsConfig;
        this.annotation = annotation;
    }

    @Inject
    public void injectInjector(Injector injector) {
        TypeLiteral<Set<JmsUriInterceptor>> type = new TypeLiteral<Set<JmsUriInterceptor>>() {};
        final Binding<Set<JmsUriInterceptor>> binding;

        binding = injector.getExistingBinding(Key.get(type, annotation));

        if (binding != null) {
            interceptors = binding.getProvider().get();
        }
    }

    @Override
    public ConnectionFactory get()
    {
        String jmsUriStr = jmsConfig.getJmsConnectionUrlFormat();

        if (StringUtils.isBlank(jmsUriStr)) {
            throw new ProvisionException(format("JMS URI for %s can not be blank!", annotation == null ? "<default>" : annotation));
        }

        for (JmsUriInterceptor interceptor : interceptors) {
            jmsUriStr = interceptor.apply(jmsUriStr);
        }

        URI jmsUri = URI.create(jmsUriStr);

        return new ActiveMQConnectionFactory(jmsUri);
    }
}
