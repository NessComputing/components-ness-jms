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

import javax.jms.ConnectionFactory;


import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.nesscomputing.config.Config;
import com.nesscomputing.jms.activemq.DiscoveryJmsUriInterceptor;
import com.nesscomputing.logging.Log;

/**
 * Provides access to JMS for the Ness platform.
 */
public class JmsModule extends AbstractModule
{
    private static final Log LOG = Log.findLog();

    private final String connectionName;
	private final Config config;

	public JmsModule(final Config config)
    {
	    this(config, null);
	}

	public JmsModule(final Config config, final String connectionName)
    {
	    this.config = config;
        this.connectionName = connectionName;
	}

	@Override
	protected void configure()
    {
        final Named connectionNamed;
        final JmsConfig jmsConfig;


	    if (connectionName == null) {
            connectionNamed = null;
	        jmsConfig = config.getBean(JmsConfig.class);
	        bind(JmsConfig.class).toInstance(jmsConfig);
	    }
	    else {
	        connectionNamed = Names.named(connectionName);
	        jmsConfig = config.getBean(JmsConfig.class, ImmutableMap.of("name", connectionName));
	        bind(JmsConfig.class).annotatedWith(connectionNamed).toInstance(jmsConfig);
	    }

	    bind(JsonProducerCallback.class).in(Scopes.SINGLETON);

	    if (jmsConfig.isEnabled()) {
	        LOG.info("Enabling JMS for '%s'", Objects.firstNonNull(connectionName, "<default>"));

	        if (connectionName == null) {
	            bind(ConnectionFactory.class).toProvider(new ActiveMQConnectionFactoryProvider(jmsConfig, null)).in(Scopes.SINGLETON);
	            bind(JmsRunnableFactory.class).toInstance(new JmsRunnableFactory(null));
	        }
	        else {
                bind(ConnectionFactory.class).annotatedWith(connectionNamed).toProvider(new ActiveMQConnectionFactoryProvider(jmsConfig, connectionNamed)).in(Scopes.SINGLETON);
                bind(JmsRunnableFactory.class).annotatedWith(connectionNamed).toInstance(new JmsRunnableFactory(connectionNamed));
	        }
	    }
	    else {
            LOG.info("Disabled JMS for '%s'", Objects.firstNonNull(connectionName, "<default>"));
	    }

	    if (jmsConfig.isSrvcTransportEnabled()) {
	        if (connectionNamed == null) {
	            Multibinder.newSetBinder(binder(), JmsUriInterceptor.class).addBinding().to(DiscoveryJmsUriInterceptor.class);
	        } else {
	            Multibinder.newSetBinder(binder(), JmsUriInterceptor.class, connectionNamed).addBinding().to(DiscoveryJmsUriInterceptor.class);
	        }
	    }
	}
}
