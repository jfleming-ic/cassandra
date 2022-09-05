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

package org.apache.cassandra.config;

import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;

import org.slf4j.LoggerFactory;

import static java.lang.String.format;

public class ObjectValuesParametrizedClass
{
    public static final String CLASS_NAME = "class_name";
    public static final String PARAMETERS = "parameters";

    public String class_name;
    public Map<String, Object> parameters;

    public static int resolveInteger(Map<String, Object> parameters,
                                     String path,
                                     String key,
                                     Integer defaultValue)
    {
        Object resolveLength = parameters.getOrDefault(key, defaultValue.toString());

        try
        {
            if (resolveLength instanceof Integer) {
                return (Integer) resolveLength;
            }
            if (resolveLength instanceof String) {
                return Integer.parseInt((String) resolveLength);
            }
            throw new IllegalStateException();
        }
        catch (IllegalStateException | NumberFormatException ex)
        {
            LoggerFactory.getLogger(ObjectValuesParametrizedClass.class)
                         .warn(format("Unable to parse %s of %s from value '%s'. Value has to be integer. " +
                                      "The default of value %s will be used.",
                                      key, path, resolveLength, defaultValue));
        }
        return defaultValue;
    }

    public ObjectValuesParametrizedClass()
    {
        // for snakeyaml
    }

    @SuppressWarnings("unchecked")
    public ObjectValuesParametrizedClass(Map<String, ?> p)
    {
        this((String)p.get(CLASS_NAME),
             p.containsKey(PARAMETERS) ? (Map<String, Object>)((List<?>)p.get(PARAMETERS)).get(0) : null);
    }

    public ObjectValuesParametrizedClass(String class_name, Map<String, Object> parameters)
    {
        this.class_name = class_name;
        this.parameters = parameters;
    }

    @Override
    public boolean equals(Object that)
    {
        return that instanceof ObjectValuesParametrizedClass && equals((ObjectValuesParametrizedClass) that);
    }

    public boolean equals(ParameterizedClass that)
    {
        return Objects.equal(class_name, that.class_name) && Objects.equal(parameters, that.parameters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(class_name, parameters);
    }

    @Override
    public String toString()
    {
        return class_name + (parameters == null ? "" : parameters.toString());
    }
}
