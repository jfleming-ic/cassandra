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

import java.util.Optional;

import javax.annotation.Nullable;

import org.apache.cassandra.service.ClientState;

public class CustomGuardrail<VALUE> extends Guardrail
{
    private final ValueValidator<VALUE> validator;
    private final boolean guardWhileSuperuser;

    public CustomGuardrail(String name,
                           CustomGuardrailConfig config,
                           Class<? extends ValueValidator<VALUE>> defaultValidatorClass,
                           boolean guardWhileSuperuser)
    {
        super(name);

        String validatorClass = config.resolveString("class_name", defaultValidatorClass.getCanonicalName());
        ValueValidator<VALUE> validator = Guardrails.newGuardrailValidator(validatorClass, config);
        validator.validateParameters();

        this.validator = validator;
        this.guardWhileSuperuser = guardWhileSuperuser;
    }

    @Override
    public boolean enabled(@Nullable ClientState state)
    {
        return guardWhileSuperuser ? super.enabled(null) : super.enabled(state);
    }

    public void guard(VALUE value, ClientState state)
    {
        if (!enabled(state))
            return;

        Optional<String> failMessage = validator.shouldFail(value);

        if (failMessage.isPresent())
            fail(failMessage.get(), state);
        else
            validator.shouldWarn(value).ifPresent(this::warn);
    }
}
