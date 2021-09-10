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

package org.apache.cassandra.diag;

import java.io.InvalidClassException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.diag.store.DiagnosticEventMemoryStore;
import org.apache.cassandra.diag.store.DiagnosticEventStore;


/**
 * Manages storing and retrieving events based on enabled {@link DiagnosticEventStore} implementation.
 */
public final class DiagnosticEventPersistence
{
    private static final Logger logger = LoggerFactory.getLogger(DiagnosticEventPersistence.class);

    private static final DiagnosticEventPersistence instance = new DiagnosticEventPersistence();

    private final Map<Class, DiagnosticEventStore<Long>> stores = new ConcurrentHashMap<>();

    private final Consumer<DiagnosticEvent> eventConsumer = this::onEvent;

    public static void start()
    {
        // make sure id broadcaster is initialized (registered as MBean)
        LastEventIdBroadcaster.instance();
    }

    public static DiagnosticEventPersistence instance()
    {
        return instance;
    }

    public SortedMap<Long, Map<String, Serializable>> getEvents(Long key, int limit, boolean includeKey, boolean mustBeEnabled)
    {
        final SortedMap<Long, Map<String, Serializable>> result = new TreeMap<>();
        stores.entrySet()
              .stream()
              .filter(entry -> !mustBeEnabled || DiagnosticEventService.instance().isEnabled(entry.getKey()))
              .forEach(new Consumer<Map.Entry<Class, DiagnosticEventStore<Long>>>()
              {
                  @Override
                  public void accept(Map.Entry<Class, DiagnosticEventStore<Long>> classDiagnosticEventStoreEntry)
                  {
                      result.putAll(getEvents(classDiagnosticEventStoreEntry.getKey().getName(), key, limit, includeKey, mustBeEnabled));
                  }
              });
        return result;
    }

    public SortedMap<Long, Map<String, Serializable>> getEvents(String eventClazz, Long key, int limit, boolean includeKey, boolean mustBeEnabled)
    {
        assert eventClazz != null;
        assert key != null;
        assert limit >= 0;

        Class cls;
        try
        {
            cls = getEventClass(eventClazz);
        }
        catch (ClassNotFoundException | InvalidClassException e)
        {
            throw new RuntimeException(e);
        }

        if (mustBeEnabled && !DiagnosticEventService.instance().isEnabled(cls))
        {
            return Collections.emptySortedMap();
        }

        DiagnosticEventStore<Long> store = getStore(cls);

        NavigableMap<Long, DiagnosticEvent> events = store.scan(key, includeKey ? limit : limit + 1);
        if (!includeKey && !events.isEmpty()) events = events.tailMap(key, false);
        final TreeMap<Long, Map<String, Serializable>> ret = new TreeMap<>();
        for (Map.Entry<Long, DiagnosticEvent> entry : events.entrySet())
        {
            DiagnosticEvent event = entry.getValue();
            HashMap<String, Serializable> val = new HashMap<>(event.toMap());
            val.put("class", event.getClass().getName());
            val.put("type", event.getType().name());
            val.put("ts", event.timestamp);
            val.put("thread", event.threadName);
            ret.put(entry.getKey(), val);
        }
        logger.debug("Returning {} {} events for key {} (limit {}) (includeKey {})", ret.size(), eventClazz, key, limit, includeKey);
        return ret;
    }

    public SortedMap<Long, Map<String, Serializable>> getEvents(String eventClazz, Long key, int limit, boolean includeKey)
    {
        return getEvents(eventClazz, key, limit, includeKey, false);
    }

    public void enableEventPersistence(String eventClazz)
    {
        enableEventPersistence(eventClazz, DatabaseDescriptor.diagnosticEventsVTableEnabled());
    }

    public void enableEventPersistence(String eventClazz, boolean updateVTable)
    {
        try
        {
            logger.debug("Enabling events: {}", eventClazz);
            Class<DiagnosticEvent> clazz = getEventClass(eventClazz);
            DiagnosticEventService.instance().subscribe(clazz, eventConsumer);

            if (updateVTable && DatabaseDescriptor.diagnosticEventsVTableEnabled())
                updateEventInDiagnosticsVTable(clazz);
        }
        catch (ClassNotFoundException | InvalidClassException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void disableEventPersistence(String eventClazz, boolean updateVtable)
    {
        try
        {
            logger.debug("Disabling events: {}", eventClazz);
            Class<DiagnosticEvent> clazz = getEventClass(eventClazz);
            DiagnosticEventService.instance().unsubscribe(clazz, eventConsumer);

            if (updateVtable && DatabaseDescriptor.diagnosticEventsVTableEnabled())
                updateEventInDiagnosticsVTable(clazz);
        }
        catch (ClassNotFoundException | InvalidClassException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void disableEventPersistence(String eventClazz)
    {
        disableEventPersistence(eventClazz, DatabaseDescriptor.diagnosticEventsVTableEnabled());
    }

    private void updateEventInDiagnosticsVTable(Class<DiagnosticEvent> eventClazz)
    {
        QueryProcessor.executeOnceInternal("UPDATE system_views.diagnostic SET enabled = ? WHERE event = ?;",
                                           DiagnosticEventService.instance().hasSubscribers(eventClazz),
                                           eventClazz.getName());
    }

    private void onEvent(DiagnosticEvent event)
    {
        Class<? extends DiagnosticEvent> cls = event.getClass();
        if (logger.isTraceEnabled())
            logger.trace("Persisting received {} event", cls.getName());
        DiagnosticEventStore<Long> store = getStore(cls);
        store.store(event);
        LastEventIdBroadcaster.instance().setLastEventId(event.getClass().getName(), store.getLastEventId());
    }

    private Class<DiagnosticEvent> getEventClass(String eventClazz) throws ClassNotFoundException, InvalidClassException
    {
        // get class by eventClazz argument name
        // restrict class loading for security reasons
        if (!eventClazz.startsWith("org.apache.cassandra."))
            throw new RuntimeException("Not a Cassandra event class: " + eventClazz);

        Class<DiagnosticEvent> clazz = (Class<DiagnosticEvent>) Class.forName(eventClazz);

        if (!(DiagnosticEvent.class.isAssignableFrom(clazz)))
            throw new InvalidClassException("Event class must be of type DiagnosticEvent");

        return clazz;
    }

    private DiagnosticEventStore<Long> getStore(Class cls)
    {
        return stores.computeIfAbsent(cls, (storeKey) -> new DiagnosticEventMemoryStore());
    }
}
