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

import java.util.Map;
import java.util.Optional;

import org.apache.cassandra.exceptions.ConfigurationException;

import static java.lang.String.format;
import static org.apache.cassandra.config.ObjectValuesParametrizedClass.resolveInteger;

public class DefaultPasswordValidator implements PasswordValidator
{
    private static final Integer DEFAULT_MIN_LENGTH_WARN = 8;
    private static final Integer DEFAULT_MIN_LENGTH_FAIL = 5;
    private static final String MIN_LENGTH_WARN_KEY = "min_length_warn";
    private static final String MIN_LENGTH_FAIL_KEY = "min_length_fail";

    private final String validPasswordFormat;

    private final int minLengthWarn;
    private final int minLengthFail;

    public DefaultPasswordValidator(Map<String, Object> parameters)
    {
        this.minLengthWarn = resolveInteger(parameters, "password_validator.parameters", MIN_LENGTH_WARN_KEY, DEFAULT_MIN_LENGTH_WARN);
        this.minLengthFail = resolveInteger(parameters, "password_validator.parameters", MIN_LENGTH_FAIL_KEY, DEFAULT_MIN_LENGTH_FAIL);

        validateParameters();

        this.validPasswordFormat = resolveValidPasswordFormat();
    }

    @Override
    public Optional<String> shouldWarn(String password)
    {
        if (password.length() < minLengthWarn)
        {
            String message = format("Warning occured when validating a password. You have provided password of length %s. %s",
                                    password.length(),
                                    validPasswordFormat);

            return Optional.of(message);
        }

        return Optional.empty();
    }

    @Override
    public Optional<String> shouldFail(String password)
    {
        if (password.length() < minLengthFail)
        {
            String message = format("Failure occured when validating a password. You have provided password of length %s. %s",
                                    password.length(),
                                    validPasswordFormat);

            return Optional.of(message);
        }

        return Optional.empty();
    }

    @Override
    public void validateParameters() throws ConfigurationException
    {
        if (minLengthWarn <= minLengthFail)
        {
            throw new ConfigurationException(format("%s of value %s is less or equal %s of value %s",
                                                    MIN_LENGTH_WARN_KEY, minLengthWarn,
                                                    MIN_LENGTH_FAIL_KEY, minLengthFail));
        }
    }

    private String resolveValidPasswordFormat()
    {
        return format("Password should be at least %s characters long and it can not be shorter than %s characters.",
                      minLengthWarn,
                      minLengthFail);
    }
}
