/**
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

package org.apache.hadoop.net;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This is a base class for hostname to topology mappings. <p/> It is not mandatory to
 * derive {@link DNSToSwitchMapping} implementations from it, but it is strongly
 * recommended, as it makes it easy for the Hadoop developers to add new methods
 * to this base class that are automatically picked up by all implementations.
 * <p/>
 *
 * This class does not extend the <code>Configured</code>
 * base class, and should not be changed to do so, as it causes problems
 * for subclasses. The constructor of the <code>Configured</code> calls
 * the  {@link #setConf(Configuration)} method, which will call into the
 * subclasses before they have been fully constructed.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class AbstractTopologyMapping
    implements DNSToSwitchMapping, Configurable {

  private Configuration conf;

  /**
   * Create an unconfigured instance
   */
  protected AbstractTopologyMapping() {
  }

  /**
   * Create an instance, caching the configuration file.
   * This constructor does not call {@link #setConf(Configuration)}; if
   * a subclass extracts information in that method, it must call it explicitly.
   * @param conf the configuration
   */
  protected AbstractTopologyMapping(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Predicate that indicates that the topology mapping is known to be
   * flat. The base class returns false: it assumes all mappings are
   * multi-rack. Subclasses may override this with methods that are more aware
   * of their topologies.
   *
   * <p/>
   *
   * This method is used when parts of Hadoop need know whether to apply
   * flat vs. hierarchical policies, such as during block placement.
   * Such algorithms behave differently if they are on tree-structured
   * layouts. 
   * </p>
   *
   * Topology-aware applications usually query this during initialization and retain the result
   * -they do not expect topology structures to change. For this reason, implementations
   * should read their settings and return true if they are confident that the current topology is
   * flat -and that it is going to stay that way. If they are unsure, return false.
   * @return true if the mapping thinks that it is -and will continue to be- flat 
   */
  public boolean isFlatTopology() {
    return false;
  }

  /**
   * Get a copy of the map (for diagnostics)
   * @return a clone of the map or null for none known
   */
  public Map<String, String> getTopologyMap() {
    return null;
  }

  /**
   * Generate a string listing the switch mapping implementation,
   * the mapping for every known node and the number of nodes and
   * unique switches known about -each entry to a separate line.
   * @return a string that can be presented to the ops team or used in
   * debug messages.
   */
  public String dumpTopology() {
    Map<String, String> rack = getTopologyMap();
    StringBuilder builder = new StringBuilder();
    builder.append("Mapping: ").append(toString()).append("\n");
    if (rack != null) {
      builder.append("Map:\n");
      Set<String> racks = new HashSet<String>();
      for (Map.Entry<String, String> entry : rack.entrySet()) {
        builder.append("  ")
            .append(entry.getKey())
            .append(" -> ")
            .append(entry.getValue())
            .append("\n");
        racks.add(entry.getValue());
      }
      builder.append("Nodes: ").append(rack.size()).append("\n");
      builder.append("Racks: ").append(racks.size()).append("\n");
      if (racks.size()>1 && racks.contains(NetworkTopology.DEFAULT_RACK)) {
        builder.append("Warning: some elements in this topology are mapped to the 'default rack'.\n");
        builder.append("         This means the topology class does not know their location.\n");
      }
    } else {
      builder.append("No topology information.");
    }
    return builder.toString();
  }

  /**
   * This method teases out the logic for querying whether or not a topology
   * is flat by looking for the script filename in the configuration.
   * This is the probe used in pre-2.x Hadoop, irrespective of the actual
   * implementation class.
   * @return true iff there is a script filename key in the configuration.
   */
  protected boolean isFlatTopologyByScriptPolicy() {
    return conf != null
        && conf.get(CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY) == null;
  }

  /**
   * Query for a {@link DNSToSwitchMapping} instance having a flat topology
   * <p/>
   * This predicate assumes that all mappings not derived from
   * the class {@link AbstractTopologyMapping} aren't flat, as they are custom classes that
   * implement specific logic. This only makes sense if the authors want non-standard mappings,
   * which usually means more complex topologies. 
   * <p/>
   * Instances of this this class are probed with the {@link #isFlatTopology()} method.
   * Topology-aware applications usually query this during initialization and retain the result
   * -they do not expect topology structures to change. 
   * @param mapping the mapping to query
   * @return true if the base class says it is single switch, or the mapping
   * is not derived from this class.
   */
  public static boolean isTopologyFlat(DNSToSwitchMapping mapping) {
    return mapping != null && mapping instanceof AbstractTopologyMapping
        && ((AbstractTopologyMapping) mapping).isFlatTopology();
  }

}
