/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.guardrails;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.utils.Clock;

import static java.lang.String.format;
import static org.apache.cassandra.auth.AuthKeyspace.PREVIOUS_PASSWORDS;
import static org.apache.cassandra.auth.CassandraAuthorizer.authReadConsistencyLevel;
import static org.apache.cassandra.cql3.QueryProcessor.execute;
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;

public class PasswordFreshnessGuardrail extends Guardrail
{
    private final FreshnessPredicate freshnessPredicate;

    public PasswordFreshnessGuardrail(String name, GuardrailsConfigProvider configProvider)
    {
        super(name);
        this.freshnessPredicate = new FreshnessPredicate(configProvider);
    }

    @Override
    public boolean enabled(@Nullable ClientState state)
    {
        return super.enabled(null);
    }

    public void guard(String roleName, Message.Response response, ClientState state)
    {
        if (!enabled(state))
            return;

        if (!freshnessPredicate.apply(state).test(roleName))
        {
            if (response.getWarnings() == null)
                response.setWarnings(new ArrayList<>());

            response.getWarnings().add("too old password!");
        }
    }

    private static final class FreshnessPredicate implements Function<ClientState, Predicate<String>>
    {
        private final GuardrailsConfigProvider configProvider;

        public FreshnessPredicate(GuardrailsConfigProvider configProvider)
        {
            this.configProvider = configProvider;
        }

        @Override
        public Predicate<String> apply(final ClientState state)
        {
            return roleName -> {
                long passwordFreshnessThreshold = configProvider.getOrCreate(state).getPasswordFreshnessThreshold();

                if (passwordFreshnessThreshold == 0)
                    return false;

                return checkPasswordFreshness(roleName, passwordFreshnessThreshold);
            };
        }

        private boolean checkPasswordFreshness(String roleName, long passwordFreshnessThreshold)
        {
            if (roleName.equals("cassandra"))
                return true;

            UntypedResultSet rows = execute(format("SELECT created FROM %s.%s WHERE role = ? ORDER BY created DESC LIMIT 1",
                                                   AUTH_KEYSPACE_NAME,
                                                   PREVIOUS_PASSWORDS),
                                            authReadConsistencyLevel(),
                                            roleName);

            if (rows != null && !rows.isEmpty())
            {
                UntypedResultSet.Row row = rows.one();
                if (row.has("created"))
                {
                    long created = row.getTimeUUID("created").unix(TimeUnit.MILLISECONDS);
                    long current = Clock.Global.currentTimeMillis();

                    return current - (passwordFreshnessThreshold * 1000) < created;
                }
            }

            return true;
        }
    }
}
