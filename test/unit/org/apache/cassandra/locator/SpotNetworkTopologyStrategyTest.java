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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class SpotNetworkTopologyStrategyTest extends AbstractTokenAllocationTest
{

    @Test
    public void testEnsureQuorumPerDcNotSpots()
    {
        ContextHolder holder = prepare(3, 9, 5, 256, 0, 3, 6);

        AbstractReplicationStrategy strategy = new SpotNetworkTopologyStrategy("ks1",
                                                                               holder.metadata,
                                                                               holder.snitch,
                                                                               "dc1", "5",
                                                                               "ensure_quorum_not_spots", "true");

        // max two spots as quorum of normals from 5 is 3, so 5 - 3 = 2
        assertEquals(0, checkMaxSpotsViolations(strategy, 2));

        holder = prepare(3, 3, 3, 256, 1);

        strategy = new SpotRackAwareTopologyStrategy("ks1",
                                                     holder.metadata,
                                                     holder.snitch,
                                                     "dc1", "3",
                                                     "ensure_quorum_not_spots", "true");

        assertEquals(0, checkMaxSpotsViolations(strategy, 1));
    }

    @Test
    public void testMaxSpots()
    {
        for (int maxSpots = 0; maxSpots <= 3; maxSpots++)
        {
            ContextHolder holder = prepare(3, 9, 3, 256, 0, 3, 6);

            AbstractReplicationStrategy strategy = new SpotNetworkTopologyStrategy("ks1",
                                                                                   holder.metadata,
                                                                                   holder.snitch,
                                                                                   "dc1", "5",
                                                                                   "max_spots", Integer.toString(maxSpots));

            assertEquals(0, checkMaxSpotsViolations(strategy, maxSpots));
        }
    }

    @Test
    public void spotNetworkTopologyAllocationTestWithoutSpots()
    {
        ContextHolder holder = prepare(3, 3, 3, 256);
        AbstractReplicationStrategy strategy = new SpotNetworkTopologyStrategy("ks1", holder.metadata, holder.snitch, "dc1", "3");

        assertEquals(0, checkViolations(holder, strategy, 0));
    }

    @Test
    public void spotNetworkTopologyAllocationTest()
    {
        ContextHolder holder = prepare(3, 9, 5, 256, 0, 3, 6);
        AbstractReplicationStrategy strategy = new SpotNetworkTopologyStrategy("ks1", holder.metadata, holder.snitch, "dc1", "5");

        assertNotEquals(0, checkViolations(holder, strategy, 3));
    }

    @Test
    public void spotNetworkTopologyAllocation2Test()
    {
        ContextHolder holder = prepare(4, 12, 5, 256, 0, 3, 6, 9);
        AbstractReplicationStrategy strategy = new SpotNetworkTopologyStrategy("ks1", holder.metadata, holder.snitch, "dc1", "5");

        assertEquals(0, checkViolations(holder, strategy, 4));
    }

    @Test
    public void spotNetworkTopologyAllocation3Test()
    {
        ContextHolder holder = prepare(3, 12, 5, 256, 0, 4, 8);
        AbstractReplicationStrategy strategy = new SpotNetworkTopologyStrategy("ks1", holder.metadata, holder.snitch, "dc1", "5");

        assertNotEquals(0, checkViolations(holder, strategy, 4));
    }
}
