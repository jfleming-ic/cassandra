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

import org.apache.cassandra.exceptions.ConfigurationException;

public interface ValueValidator<VALUE>
{
    /**
     * Test a value to see if it emits warnings.
     *
     * @param value value to validate
     * @return if optional is empty, value is valid, otherwise it returns warning violation message
     */
    Optional<String> shouldWarn(VALUE value);

    /**
     * Test a value to see if it emits failures.
     *
     * @param value value to validate
     * @return if optional is empty, value is valid, otherwise it returns failure violation message
     */
    Optional<String> shouldFail(VALUE value);

    /**
     * Validates parameters for this validator.
     *
     * @throws ConfigurationException in case configuration for this specific validator is invalid
     */
    void validateParameters() throws ConfigurationException;
}
