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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY;

/**
 * Provides an entry point for command line topology operations.
 * The initial operations allow callers to test different topology
 * implementations and configurations  -and to see the results of
 * resolving hostnames/IP addresses against them.
 */
public class TopologyTool extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(TopologyTool.class);

  public TopologyTool() {
  }

  public TopologyTool(Configuration conf) {
    super(conf);
  }


  /**
   * Print a line of text out.
   * @param text text to print
   */
  protected void println(CharSequence text) {
    System.out.println(text);
  }

  /**
   * Print the usage messages
   */
  private void usage() {
    println("usage: hadoop topo test [hostname] [hostname] ...");
    println("usage: hadoop topo testfile filename");
  }

  @Override
  public int run(String[] args) throws Exception {

    if (args.length < 1) {
      usage();
      return -1;
    }

    String operation = args[0];

    Configuration conf = getConf();
    String mapclass = conf.get(NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY);
    println(NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY + "=" + mapclass);
    AbstractDNSToSwitchMapping mapping = null;
    try {
      mapping = AbstractDNSToSwitchMapping.
          createCachingDNSToSwitchMapping(conf);
    } catch (Exception e) {
      //classloader failures. Bail out and provide a hint of the cause
      LOG.error("Failed to load the DNS mapping " + mapclass
            +  ": " + e, e);
      LOG.error("The configuration option is wrong, or the classpath is incomplete");
      return -1;
    }
    println("Caching wrapper class = " + mapping.getClass());
    String script = conf.get(
        CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY);
    if (script != null) {
      println("Mapping script filename= \"" + script + "\"");
      File scriptFile = new File(script);
      println("Mapping script path = \"" + scriptFile.getAbsolutePath() + "\"");
      if (!scriptFile.exists() &&
          "org.apache.hadoop.net.ScriptBasedMapping".equals(mapclass)) {
        LOG.warn(
            "Script file not found -the script must be in the execution path");
      }
    }
    println("Instance information: " + mapping);
    boolean singleSwitch =
        AbstractDNSToSwitchMapping.isMappingSingleSwitch(mapping);
    println("Topology is " + (singleSwitch ? "" : "not  ") +
                "considered single-switch");

    boolean successful;
    if ("test".equals(operation)) {
      successful = resolveHostnames(mapping, args, 1);
    } else if ("testfile".equals(operation)) {
      if (args.length != 2) {
        usage();
        successful = false;
      } else {
        String filename = args[1];
        successful = resolveHostFile(mapping, filename);
      }
    } else {
      usage();
      successful = false;
    }

    return successful ? 0 : 1;
  }

  /**
   * Print the topology held by the mapping
   *
   * @param mapping mapping to print
   */
  private void printTopology(AbstractDNSToSwitchMapping mapping) {
    println("\nFinal topology:\n");
    String topology = mapping.dumpTopology();
    println(topology);
  }

  /**
   * Load a file and hand its hostnames off for mapping
   *
   * @param mapping the mapping implementation
   * @param filename the file to load
   * @return true if it was considered successful
   * @throws IOException on any IO problem
   */
  private boolean resolveHostFile(AbstractDNSToSwitchMapping mapping,
                                  String filename) throws IOException {
    BufferedReader reader = null;
    List<String> hostnames = new ArrayList<String>();
    try {
      reader = new BufferedReader(new FileReader(filename));
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (!line.isEmpty()) {
          hostnames.add(line);
        }
      }
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          LOG.warn("Error closing " + filename + " : " + e, e);
        }
      }
    }
    String hosts[] = new String[hostnames.size()];
    hostnames.toArray(hosts);
    return resolveHostnames(mapping, hosts, 0);
  }

  /**
   * Test the array of hosts (or a subset thereof) for resolving in the mapper.
   * The results are printed during the process; the final topology is then
   * displayed.
   *
   * @param mapping the mapping to test
   * @param hosts the hostnames
   * @param offset an offset into the array: 0 means start from the beginning
   * @return
   */
  private boolean resolveHostnames(AbstractDNSToSwitchMapping mapping,
                                   String[] hosts,
                                   int offset) {
    int failures = 0;
    List<String> hostnameList = new ArrayList<String>(1);
    hostnameList.add("");
    int l = hosts.length;
    for (int i = offset; i < l; i++) {
      String hostname = hosts[i];
      hostnameList.set(0, hostname);
      try {
        println("Resolving " + hostname);
        long starttime = System.nanoTime();
        List<String> resolved = mapping.resolve(hostnameList);
        long endtime = System.nanoTime();
        if (resolved == null) {
          LOG.warn("Hostname resolution returned a null list");
          failures++;
        } else if (resolved.size() != 1) {
          LOG.warn("hostname resolved to a list of size " + resolved.size());
          failures++;
        } else {
          StringBuilder builder = new StringBuilder();
          builder.append("Hostname \"").append(hostname)
              .append("\" resolved to ")
              .append('"').append(resolved.get(0)).append("\" ");
          double duration = (endtime - starttime) / 1e6;
          builder.append(" in ").append(duration).append(" milliseconds");
          println(builder);
        }
      } catch (Exception e) {
        LOG.error("Error while trying to resolve host " + hostname
                      + ": " + e,
                  e);
        failures++;
      }
    }
    //now dump the topology
    printTopology(mapping);
    return failures == 0;
  }

  /**
   * Entry point
   *
   * @param argv the command and its arguments
   */
  public static void main(String argv[]) {
    TopologyTool topo = new TopologyTool();
    int res;
    try {
      res = ToolRunner.run(topo, argv);
    } catch (Throwable e) {
      LOG.error("Failure: " + e, e);
      res = 1;
    }
    System.exit(res);
  }
}
