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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.locator.InetAddressAndPort.getByAddress;

public class InstaclustrTopologyTest
{
    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(new Murmur3Partitioner());
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
    }

    public static class TestingSnitch extends AbstractNetworkTopologySnitch
    {

        Map<InetAddressAndPort, NodeInfo> nodes = new HashMap<>();

        public TestingSnitch(Map<InetAddressAndPort, NodeInfo> nodes)
        {
            this.nodes.putAll(nodes);
        }

        @Override
        public String getRack(InetAddressAndPort endpoint)
        {
            return nodes.get(endpoint).rack;
        }

        @Override
        public String getDatacenter(InetAddressAndPort endpoint)
        {
            return nodes.get(endpoint).dc;
        }
    }

    private static class NodeInfo
    {
        String dc;
        String rack;
        Long initialToken;
        Set<String> tag;

        public NodeInfo(String dc, String rack, Long initialToken, String tag)
        {
            this.dc = dc;
            this.rack = rack;
            this.initialToken = initialToken;
            this.tag = Collections.singleton(tag);
        }

        public static NodeInfo create(String dc, String rack, Long initialToken)
        {
            return create(dc, rack, initialToken, null);
        }

        public static NodeInfo create(String dc, String rack, Long initialToken, String tag)
        {
            return new NodeInfo(dc, rack, initialToken, tag);
        }
    }

    @Test
    public void test() throws Exception
    {
        Map<InetAddressAndPort, NodeInfo> nodes = new HashMap<InetAddressAndPort, NodeInfo>() {{
            put(getByAddress(new byte[] {(byte) 172, 19, 0, 5}) , NodeInfo.create("dc1", "rack1", 6504358681601109713L, "spot"));
            put(getByAddress(new byte[] {(byte) 172, 19, 0, 8}) , NodeInfo.create("dc1", "rack2", -734101840898117784L));
            put(getByAddress(new byte[] {(byte) 172, 19, 0, 9}) , NodeInfo.create("dc1", "rack3", -5131980963979693427L));
            put(getByAddress(new byte[] {(byte) 172, 19, 0, 10}), NodeInfo.create("dc1", "rack1", 3664100468419689585L, "spot"));
            put(getByAddress(new byte[] {(byte) 172, 19, 0, 11}), NodeInfo.create("dc1", "rack2", -7804505472323707473L));
            put(getByAddress(new byte[] {(byte) 172, 19, 0, 12}), NodeInfo.create("dc1", "rack3", 1464999313760785900L));
            put(getByAddress(new byte[] {(byte) 172, 19, 0, 15}), NodeInfo.create("dc1", "rack1", 8573298641493476927L, "spot"));
            put(getByAddress(new byte[] {(byte) 172, 19, 0, 16}), NodeInfo.create("dc1", "rack2",-2933041402438905606L));
            put(getByAddress(new byte[] {(byte) 172, 19, 0, 17}), NodeInfo.create("dc1", "rack3", 5084229575010399649L));
        }};

        IEndpointSnitch snitch = new TestingSnitch(nodes);
        DatabaseDescriptor.setEndpointSnitch(snitch);

        TokenMetadata metadata = new TokenMetadata(snitch);
        metadata.clearUnsafe();

        for (Map.Entry<InetAddressAndPort, NodeInfo> entry : nodes.entrySet())
        {
            Token token = metadata.partitioner.getTokenFactory().fromString(entry.getValue().initialToken.toString());
            metadata.updateTopology(entry.getKey());
            metadata.updateNormalToken(token, entry.getKey());
            metadata.updateTag(entry.getKey(), entry.getValue().tag);
        }

        Map<String, String> configOptions = new HashMap<String, String>();
        configOptions.put("dc1", "5");

        NetworkTopologyStrategy strategy = new NetworkTopologyStrategy("ks1", metadata, snitch, configOptions);

        TableMetadata.Builder builder = TableMetadata.builder("ks1", "tb1")
                                                     .addPartitionKeyColumn("id", UTF8Type.instance);
        TableMetadata tmd = builder.build();

        for (int i = 1; i < 10; i++)
        {
            Token token = metadata.partitioner.getToken(tmd.partitionKeyType.fromString(Integer.toString(i)));
            EndpointsForToken endpoints = strategy.getNaturalReplicasForToken(token);
            System.out.println(endpoints);
        }
    }
}
