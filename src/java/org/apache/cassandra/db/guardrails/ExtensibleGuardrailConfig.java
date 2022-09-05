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

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import org.slf4j.LoggerFactory;

import static java.lang.String.format;

public class ExtensibleGuardrailConfig extends HashMap<String, Object>
{
    public ExtensibleGuardrailConfig()
    {
        // for snakeyaml
    }

    @SuppressWarnings("unchecked")
    public ExtensibleGuardrailConfig(Map<String, ?> p)
    {
        super(p);
    }

    public String resolveString(@Nullable String key, String defaultValue)
    {
        if (key == null)
            return defaultValue;

        Object resolvedString = getOrDefault(key, defaultValue);

        if (resolvedString instanceof String)
            return (String) resolvedString;

        return resolvedString.toString();
    }

    public int resolveInteger(@Nullable String key, Integer defaultValue)
    {
        if (key == null)
            return defaultValue;

        Object resolvedValue = getOrDefault(key, defaultValue.toString());

        try
        {
            if (resolvedValue instanceof Integer) {
                return (Integer) resolvedValue;
            }
            if (resolvedValue instanceof String) {
                return Integer.parseInt((String) resolvedValue);
            }
            throw new IllegalStateException();
        }
        catch (IllegalStateException | NumberFormatException ex)
        {
            LoggerFactory.getLogger(ExtensibleGuardrailConfig.class)
                         .warn(format("Unable to parse value %s of key %s. Value has to be integer. " +
                                      "The default of value %s will be used.",
                                      resolvedValue, key, defaultValue));
        }
        return defaultValue;
    }
}
