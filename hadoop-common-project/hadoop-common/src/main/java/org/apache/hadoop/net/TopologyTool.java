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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY;

/**
 * Provides an entry point for command line topology operations.
 * The initial operations allow callers to test different topology
 * implementations and configurations  -and to see the results of
 * resolving hostnames/IP addresses against them.
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class TopologyTool extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(TopologyTool.class);

  /**
   * Name on the command line; used in error/usage text :{@value}
   */
  public static final String TOOLNAME = "topology";
  public static final int E_USAGE = -2;
  public static final int E_FAIL = -1;
  public static final String RESOLVE = "r";
  public static final String ACTION_TEST = "test";
  public static final String ACTION_TESTFILE = "testfile";
  private boolean nslookup;
  private AbstractDNSToSwitchMapping topology;

  public TopologyTool() {
  }

  public TopologyTool(Configuration conf) {
    super(conf);
  }

  /**
   * Print a line of text out, with String formatting 
   * @param text text to print
   */
  protected void println(String text, Object... args) {
    System.out.println(String.format(text, args));
  }

  /**
   * Print the usage messages
   */
  private void usage() {
    String prefix = "usage: hadoop " + TOOLNAME + " [-r" + RESOLVE + "]";
    println(prefix + " test [hostname] [hostname] ...");
    println(prefix + " testfile <hostnamefile>");
    println(
      "If the -" + RESOLVE + " option is set, the IP addresses of the hosts" +
      "are looked up and their topology also resolved");
    println("the hostnamefile must contain a list of hosts, one to a line.");
    println("Any line starting with a # symbol will be treated as a comment");
  }

  @Override
  public int run(String[] args) throws Exception {


    String operation = null;
    List<String> argsList = null;
    try {
      Options options = new Options();
      Option opt = OptionBuilder.withDescription("resolve all hostnames")
                                .create(RESOLVE);
      options.addOption(opt);
      CommandLineParser parser = new GnuParser();
      CommandLine commandLine = parser.parse(options, args, true);
      nslookup = commandLine.hasOption(RESOLVE);
      argsList = commandLine.getArgList();
      //verify that there is an action as arg #1
      if (!argsList.isEmpty()) {
        operation = argsList.remove(0);
      }
    } catch (ParseException e) {
      LOG.debug("Parse failure: " + e, e);
    }


    if (operation == null) {
      usage();
      return E_USAGE;
    }


    Configuration conf = getConf();
    String mapclass = conf.get(NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY);
    println(NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY + "=" + mapclass);
    topology = null;
    try {
      topology = AbstractDNSToSwitchMapping.createCachingDNSToSwitchMapping(
        conf);
    } catch (Exception e) {
      //classloader failures. Bail out and provide a hint of the cause
      LOG.error("Failed to load the DNS mapping " + mapclass
                + ": " + e, e);
      LOG.error(
        "The configuration option is wrong, or the classpath is incomplete");
      return E_FAIL;
    }
    println("Caching wrapper class = " + topology.getClass());
    String script = conf.get(
      CommonConfigurationKeysPublic.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY);
    if (script != null) {
      println("Mapping script filename= \"" + script + "\"");
      File scriptFile = new File(script);
      println("Mapping script path = \"" + scriptFile.getAbsolutePath() + "\"");
      if (!scriptFile.exists() && topology instanceof ScriptBasedMapping) {
        LOG.warn(
          "Script file not found -the script must be in the execution path");
      }
    }
    println("Instance information: %s", topology);
    boolean singleSwitch =
      AbstractDNSToSwitchMapping.isMappingSingleSwitch(topology);
    println("Topology is %sconsidered single-switch",
            (singleSwitch ? " " : "not"));

    boolean successful;
    if (ACTION_TEST.equals(operation)) {
      successful = resolveHostnameTopologies(argsList).isEmpty();
    } else if (ACTION_TESTFILE.equals(operation)) {
      if (args.length != 2) {
        usage();
        successful = false;
      } else {
        String filename = args[1];
        successful = resolveHostFile(filename).size() == 0;
      }
    } else {
      usage();
      successful = false;
    }

    return successful ? 0 : E_FAIL;
  }

  /**
   * Print the topology held by the mapping
   *
   * @param mapping mapping to print
   */
  private void printTopology(AbstractDNSToSwitchMapping mapping) {
    println("\nFinal topology:\n");
    println(mapping.dumpTopology());
  }

  /**
   * Load a file and hand its hostnames off for mapping
   *
   *
   * @param filename the file to load
   * @return #of failed resolutions
   * @throws IOException on any IO problem
   */
  private List<TopoResolutionPair> resolveHostFile(String filename) throws IOException {
    BufferedReader reader = null;
    List<String> hostnames = new ArrayList<String>();
    try {
      reader = new BufferedReader(new FileReader(filename));
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (!line.isEmpty() && !line.startsWith("#")) {
          hostnames.add(line);
        }
      }
    } finally {
      IOUtils.closeQuietly(reader);
    }
    String hosts[] = new String[hostnames.size()];
    hostnames.toArray(hosts);
    return resolveHostnameTopologies(hostnames);
  }

  /**
   * Test the array of hosts (or a subset thereof) for resolving in the mapper.
   * The results are printed during the process; the final topology is then
   * displayed.
   *
   *
   * @param hosts the hostnames
   * @return #of failed resolutions
   */
  private List<TopoResolutionPair> resolveHostnameTopologies(List<String> hosts) {
    int failures = 0;
    List<TopoResolutionPair> resolved =
      new ArrayList<TopoResolutionPair>(hosts.size());
    List<TopoResolutionPair> unresolved = new ArrayList<TopoResolutionPair>();
    for (String hostname : hosts) {
      TopoResolutionPair resultPair = new TopoResolutionPair();
      resolved.add(resultPair);
      TopoResult resolvedHost = resolveOneHost(hostname);
      resultPair.hostResult = resolvedHost;
      if (nslookup) {
        InetAddress ipaddr = nslookup(hostname);
        if (ipaddr != null) {
          String hostaddr = ipaddr.getHostAddress();
          println("%s has IP address %s", hostname, hostaddr);
          if (ipaddr.isLoopbackAddress()) {
            println("Warning: this is a loopback address");
          }
          TopoResult netResult = resolveOneHost(hostaddr);
          resultPair.netResult = netResult;
        } else {
          resultPair.netResult = UNKNOWN_HOST;
          println("IP address lookup failed for %s", hostname);
          failures++;
        }
      }
      
    }
    //now dump the topology
    printTopology(topology);
    return resolved;
  }

  /**
   * Resolve one host, print out resolution and time to resolve
   * @param hostname hostname
   * @return the result, null if the topo resolution failed
   */
  private TopoResult resolveOneHost(String hostname) {
    
    List<String> hostnameList = new ArrayList<String>(1);
    hostnameList.add(hostname);
    try {
      TopoResult result = new TopoResult(hostname);
      long starttime = System.nanoTime();
      List<String> resolved = topology.resolve(hostnameList);
      long endtime = System.nanoTime();
      result.resolveTimeMillis = (endtime - starttime) / 1.0e6;
      String resolvedTo = "hostname " + hostname + " resolved to ";
      if (resolved == null) {
        LOG.warn(resolvedTo + "a null list");
      } else if (resolved.size() != 1) {
        LOG.warn(resolvedTo + "a list of size " + resolved.size());
      } else {
        result.resolved = true;
        result.rack = resolved.get(0);
        StringBuilder builder = new StringBuilder();
        builder.append(resolvedTo)
               .append('"').append(resolved.get(0)).append("\" ");
        double duration = result.resolveTimeMillis;
        builder.append(" in ").append(duration).append(" milliseconds");
        println(builder.toString());
        return result;
      }
    } catch (Exception e) {
      LOG.error("Failed to resolve host " + hostname + ": " + e, e);
    }
    // only get here on a failure
    return null;
  }

  /**
   * Resolve the DNS entries of the host list, warning if they aren't found.
   * @param hosts the hostnames
   * @return the number of unresolved hostnames
   */
  /**
   * look up a hostnames IP Addr
   * @param hostname hostname to resolve
   * @return the host address or null for not found
   */
  private InetAddress nslookup(String hostname) {
    try {
      return SecurityUtil.getByName(hostname);
    } catch (UnknownHostException e) {
      return null;
    }
  }

  /**
   * Entry point
   *
   * @param argv the command and its arguments
   */
  public static void main(String argv[]) {
    try {
      TopologyTool topo = new TopologyTool();
      ExitUtil.terminate(ToolRunner.run(topo, argv));
    } catch (Throwable e) {
      LOG.error("Failure: " + e, e);
      ExitUtil.terminate(E_FAIL, e);
    }
  }
 
  
  public static class TopoResult {
    public String name;
    public String rack ="";
    public double resolveTimeMillis;
    public boolean resolved;

    public TopoResult(String name) {
      this.name = name;
    }

    /**
     * flag to indicate whether or not the node is on the rack.
     * @return true if the rack is the default one
     */
    public boolean isDefaultRack() {
      return NetworkTopology.DEFAULT_RACK.equals(rack);
    }

    @Override
    public String toString() {
      if (resolved) {
        return String.format("%s on %srack \"%s\" (resolution time =%0.3fms)",
                             name,
                             (isDefaultRack()?"default ":""),
                             rack, resolveTimeMillis);
      } else {
        return String.format("%s on unknown rack",
                             name);
      }
    }
  }

  public static TopoResult UNKNOWN_HOST;
  static {
    UNKNOWN_HOST = new TopoResult("Unknown Host");
    UNKNOWN_HOST.rack =null;
  }

  /**
   * Pair of hosts
   */
  public static class TopoResolutionPair {
    TopoResult hostResult;
    TopoResult netResult;
  }

  public static class TopoResolutionOutcome {
    public final List<TopoResolutionPair> resolved =
      new ArrayList<TopoResolutionPair>();
    public final List<TopoResolutionPair> unresolved =
      new ArrayList<TopoResolutionPair>();
    public final List<TopoResolutionPair> nslookupfailures =
      new ArrayList<TopoResolutionPair>();

    public TopoResolutionOutcome() {
    }
  }
}
