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
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.nesscomputing.config.Config;
import com.nesscomputing.logging.Log;

/**
 * Provides access to JMS for the Ness platform.
 */
public class JmsModule extends AbstractModule
{
    private static final Log LOG = Log.findLog();

    private final String connectionName;
	private final Config config;

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


        connectionNamed = Names.named(connectionName);
        jmsConfig = config.getBean(JmsConfig.class, ImmutableMap.of("name", connectionName));
        bind(JmsConfig.class).annotatedWith(connectionNamed).toInstance(jmsConfig);

	    bind(JsonProducerCallback.class).in(Scopes.SINGLETON);

	    if (jmsConfig.isEnabled()) {
	        LOG.info("Enabling JMS for '%s'", Objects.firstNonNull(connectionName, "<default>"));

            bind(ConnectionFactory.class).annotatedWith(connectionNamed).toProvider(new ActiveMQConnectionFactoryProvider(jmsConfig, connectionName, connectionNamed)).in(Scopes.SINGLETON);
            bind(JmsRunnableFactory.class).annotatedWith(connectionNamed).toInstance(new JmsRunnableFactory(connectionNamed));
	    }
	    else {
            LOG.info("Disabled JMS for '%s'", Objects.firstNonNull(connectionName, "<default>"));
	    }
	}

	// NOTE: we intentionally check if the Config is the same, we consider it an error to install two
	// different modules unless the Config is precisely the same as well.

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((config == null) ? 0 : config.hashCode());
        result = prime * result + ((connectionName == null) ? 0 : connectionName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final JmsModule other = (JmsModule) obj;
        if (config == null)
        {
            if (other.config != null)
                return false;
        } else if (!config.equals(other.config))
            return false;
        if (connectionName == null)
        {
            if (other.connectionName != null)
                return false;
        } else if (!connectionName.equals(other.connectionName))
            return false;
        return true;
    }
}
