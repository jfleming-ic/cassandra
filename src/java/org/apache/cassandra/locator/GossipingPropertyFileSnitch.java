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

package org.apache.cassandra.locator;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.SystemKeyspace.TopologyInfo;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;


public class GossipingPropertyFileSnitch extends AbstractNetworkTopologySnitch// implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(GossipingPropertyFileSnitch.class);

    private PropertyFileSnitch psnitch;

    private final String myDC;
    private final String myRack;
    private final Set<String> myTags;
    private final boolean preferLocal;
    private final AtomicReference<ReconnectableSnitchHelper> snitchHelperReference;

    private Map<InetAddressAndPort, TopologyInfo> savedEndpoints;
    private static final String DEFAULT_DC = "UNKNOWN_DC";
    private static final String DEFAULT_RACK = "UNKNOWN_RACK";

    public GossipingPropertyFileSnitch() throws ConfigurationException
    {
        SnitchProperties properties = loadConfiguration();

        myDC = properties.get("dc", DEFAULT_DC).trim();
        myRack = properties.get("rack", DEFAULT_RACK).trim();
        myTags = parseTags(properties.get("tags", null));
        preferLocal = Boolean.parseBoolean(properties.get("prefer_local", "false"));
        snitchHelperReference = new AtomicReference<>();

        try
        {
            psnitch = new PropertyFileSnitch();
            logger.info("Loaded {} for compatibility", PropertyFileSnitch.SNITCH_PROPERTIES_FILENAME);
        }
        catch (ConfigurationException e)
        {
            logger.info("Unable to load {}; compatibility mode disabled", PropertyFileSnitch.SNITCH_PROPERTIES_FILENAME);
        }
    }

    private static Set<String> parseTags(String value)
    {
        if (value == null)
            return Collections.emptySet();

        Set<String> tags = new HashSet<>();

        for (String tag : value.trim().split(","))
        {
            String trimmedTag = tag.trim();
            if (!trimmedTag.isEmpty())
                tags.add(trimmedTag);
        }

        return Collections.unmodifiableSet(tags);
    }

    private static SnitchProperties loadConfiguration() throws ConfigurationException
    {
        final SnitchProperties properties = new SnitchProperties();
        if (!properties.contains("dc") || !properties.contains("rack"))
            throw new ConfigurationException("DC or rack not found in snitch properties, check your configuration in: " + SnitchProperties.RACKDC_PROPERTY_FILENAME);

        return properties;
    }

    /**
     * Return the data center for which an endpoint resides in
     *
     * @param endpoint the endpoint to process
     * @return string of data center
     */
    public String getDatacenter(InetAddressAndPort endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            return myDC;

        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (epState == null || epState.getApplicationState(ApplicationState.DC) == null)
        {
            if (psnitch == null)
            {
                if (savedEndpoints == null)
                    savedEndpoints = SystemKeyspace.loadTopologyInfo();
                if (savedEndpoints.containsKey(endpoint))
                    return savedEndpoints.get(endpoint).dataCenter;
                return DEFAULT_DC;
            }
            else
                return psnitch.getDatacenter(endpoint);
        }
        return epState.getApplicationState(ApplicationState.DC).value;
    }

    /**
     * Return the rack for which an endpoint resides in
     *
     * @param endpoint the endpoint to process
     * @return string of rack
     */
    public String getRack(InetAddressAndPort endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            return myRack;

        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (epState == null || epState.getApplicationState(ApplicationState.RACK) == null)
        {
            if (psnitch == null)
            {
                if (savedEndpoints == null)
                    savedEndpoints = SystemKeyspace.loadTopologyInfo();
                if (savedEndpoints.containsKey(endpoint))
                    return savedEndpoints.get(endpoint).rack;
                return DEFAULT_RACK;
            }
            else
                return psnitch.getRack(endpoint);
        }
        return epState.getApplicationState(ApplicationState.RACK).value;
    }

    public Set<String> getTags(InetAddressAndPort endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            return myTags;

        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (epState == null || epState.getApplicationState(ApplicationState.TAGS) == null)
        {
            if (psnitch == null)
            {
                if (savedEndpoints == null)
                    savedEndpoints = SystemKeyspace.loadTopologyInfo();
                if (savedEndpoints.containsKey(endpoint))
                    return Optional.ofNullable(savedEndpoints.get(endpoint)).map(t -> t.tags)
                                   .orElse(Collections.emptySet());
                return Collections.emptySet();
            }
            else
                return psnitch.getTags(endpoint);
        }
        return parseTags(epState.getApplicationState(ApplicationState.TAGS).value);
    }

    public void gossiperStarting()
    {
        super.gossiperStarting();

        Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_ADDRESS_AND_PORT,
                                                   StorageService.instance.valueFactory.internalAddressAndPort(FBUtilities.getLocalAddressAndPort()));
        Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_IP,
                StorageService.instance.valueFactory.internalIP(FBUtilities.getJustLocalAddress()));

        loadGossiperState();
    }

    private void loadGossiperState()
    {
        assert Gossiper.instance != null;

        ReconnectableSnitchHelper pendingHelper = new ReconnectableSnitchHelper(this, myDC, preferLocal);
        Gossiper.instance.register(pendingHelper);

        pendingHelper = snitchHelperReference.getAndSet(pendingHelper);
        if (pendingHelper != null)
            Gossiper.instance.unregister(pendingHelper);
    }
}
