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
package org.apache.hadoop.yarn.registry;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.registry.webapp.Utils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.ProxyConfiguration;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.MapOptionHandler;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;

public class RegistryClient {
  private HttpClient _client = null;

  @Option(name="--help", aliases={ "-h" }, usage="print help message")
  private boolean _help=false;

  @Option(name="--debug", usage="run with debug on")
  private boolean _debug=false;

  @Option(name="--registry_uri", aliases={ "-R" }, metaVar="URI",
      usage="URI of the server, overrides " + Registry.REGISTRY_URI_CONF)
  private String _registryBaseURI = null;

  @Option(name="--request_headers", aliases={ "-H" },
      handler=MapOptionHandler.class)
  private Map<String, String> _requestHeaders=new HashMap<String, String>();

  @Option(name="--owners", aliases={ "-O" }, metaVar="USERS",
      handler=StringArrayOptionHandler.class,
      usage="with addvh command, e.g., \"alice bob\"")
  private String[] _owners={};

  @Option(name="--ssl-organization", metaVar="ORG",
      usage="with addvh, sets SSL cert organization")
  private String _sslOrg="storm";

  @Option(name="--ssl-organizational-unit", metaVar="ORGUNIT",
      usage="with addvh, sets SSL cert organizational unit")
  private String _sslOrgUnit="Unknown";

  private enum _Command {
    addvh, getvh, delvh
  };

  @Argument(required=true, usage="the command to run")
  private _Command _command;

  @Argument(index=1, metaVar="ARGS", multiValued=true, usage="command arguments")
  private List<String> _args = new ArrayList<String>();

  public RegistryClient(String uri, String proxy) throws IOException {
    debugMsg("Creating RegistryClient for URI "+uri+" and proxy ("+proxy+")");
    _registryBaseURI = uri;
    _client = new HttpClient();
    if (proxy != null && !proxy.isEmpty()) {
      try {
        URI proxyUri = new URI(proxy);
        _client.setProxyConfiguration(
          new ProxyConfiguration(proxyUri.getHost(), proxyUri.getPort()));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(proxy + " is not formatted correctly, should be a URI", e);
      }
    }
    try {
      _client.start();
    } catch (Exception e) {
      throw new IOException("Could not start http client", e);
    }
  }

  public void stop() throws IOException {
    try {
      _client.stop();
    } catch (Exception e) {
      throw new IOException("Could not stop http client", e);
    }
    _client = null;
  }

  private void printUsage(CmdLineParser parser) {
    System.err.println("java RegistryClient --help");
    System.err.println("java RegistryClient [options] <command> [args]");
    parser.printUsage(System.err);
    System.err.println("Commands:");
    System.err.println("  addvh [options] URI: Registers a virtual host");
    System.err.println("  getvh [options] URI: Returns information about a given VH");
    System.err.println("  delvh [options] URI: Deregisters a virtual host");
    System.err.println();
  }

  private void parseCmdLine(CmdLineParser parser, String[] args) {
    parser.setUsageWidth(80);
    try {
      parser.parseArgument(args);
      if (_args.isEmpty()) {
        throw new CmdLineException(parser, "No argument were given.");
      }
    } catch (CmdLineException e) {
      printUsage(parser);
      System.exit(1);
    }
  }

  private String serviceUriToId(URI uri) {
    return uri.getHost();
  }

  private void debugMsg(String... msgs) {
    if (_debug) {
      for (String msg : msgs) {
        System.out.println("DEBUG: " + msg);
      }
    }
  }

  private ContentResponse sendRequest(Request req) throws IOException {
    ContentResponse response = null;
    try {
      response = req.send();
    } catch (Exception e) {
      throw new IOException("Error while sending request", e);
    }

    return response;
  }

  private JsonNode getVirtualHostJsonNode(HttpClient client, String reqURI)
      throws Exception {
    Request req = client.newRequest(reqURI);

    addHeadersToReq(req);

    ContentResponse response = sendRequest(req);

    if (response.getStatus() == HttpStatus.NOT_FOUND_404) {
      return null;
    } else if (response.getStatus() != HttpStatus.OK_200) {
      System.err.println("HTTP response code: " + response.getStatus());
      throw new IOException("Errror while getting virtual host "
          + response.getContentAsString());
    }

    return Utils.parseJson(new String(response.getContent(), "UTF-8"));
  }

  public void getVirtualHost(URI serviceURI) throws Exception {
    String serviceID = serviceUriToId(serviceURI);
    debugMsg("get VH " + serviceID + " from URI " + serviceURI);
    String reqURI = _registryBaseURI.toString() + "virtualHost/"
        + URLEncoder.encode(serviceID, "UTF-8");

    try {
      JsonNode node = getVirtualHostJsonNode(_client, reqURI);
      if (node != null) {
        System.out.println(Utils.toString(node));
      }
    } finally {
      if (_client != null) {
        _client.stop();
      }
    }
  }

  public static boolean isHttps(URI uri) {
    if (uri == null) {
      return false;
    }
    return "https".equals(uri.getScheme());
  }

  private void addHeadersToReq(Request req) {
    if (_requestHeaders != null) {
      for (Map.Entry<String,String> entry: _requestHeaders.entrySet()) {
        req.header(entry.getKey(), entry.getValue());
      }
    }
  }

  private void handleOwners(URI serviceURI, ObjectNode vh, String serviceID) {
    if (isHttps(serviceURI)) {
      List<String> owners = new ArrayList<String>(Arrays.asList(_owners));

      if (owners == null || owners.size() == 0) {
        throw new IllegalArgumentException(
            "Owners is required when using HTTPS");
      }
      ArrayNode on = vh.putArray("owner");

      for (String owner : owners) {
        on.add(owner);
      }
      ObjectNode ssl = vh.putObject("securityData").putObject("SelfSignedSSL");
      ssl.put("alias", serviceID);
      ssl.put("dname",
          "CN=" + serviceID +
          ", OU= " + _sslOrgUnit +
          ", O=" + _sslOrg);
    }
  }

  public void addVirtualHost(URI serviceURI) throws Exception {
    debugMsg("service uri:" + serviceURI);

    ObjectNode root = Utils.mkNode();
    ObjectNode vh = root.putObject("virtualHost");
    String serviceID = serviceUriToId(serviceURI);
    vh.put("name", serviceID);

    handleOwners(serviceURI, vh, serviceID);

    String url = _registryBaseURI.toString() + "virtualHost";

    Request req = _client.POST(url).content(
        new BytesContentProvider(Utils.toBytes(root)), "application/json");

    addHeadersToReq(req);
    ContentResponse resp = sendRequest(req);

    if (resp.getStatus() != HttpStatus.NO_CONTENT_204) {
      System.err.println("Registry resp status (addvh):"
          + resp.getStatus());
      throw new IOException("Error while adding virtual host "
          + new String(resp.getContent(), "UTF-8"));
    }
  }

  public void delVirtualHost(URI serviceURI) throws Exception {
    debugMsg("service uri:" + serviceURI);

    String serviceID = serviceUriToId(serviceURI);

    String encodedServID = URLEncoder.encode(serviceID,"UTF-8");
    String url = _registryBaseURI.toString() + "virtualHost/" + encodedServID;

    Request req = _client.newRequest(url).method(HttpMethod.DELETE);

    addHeadersToReq(req);
    ContentResponse resp = sendRequest(req);

    if (resp.getStatus() != HttpStatus.NO_CONTENT_204) {
      System.err.println("Registry resp status (delvh):"
          + resp.getStatus());
      throw new IOException("Error while deregistering virtual host "
          + new String(resp.getContent(), "UTF-8"));
    }
  }

  private void printUsageAndExit(CmdLineParser parser, String... msgs) {
    for (String msg : msgs) {
      System.err.println(msg);
    }
    printUsage(parser);
    System.exit(1);
  }

  public void run(String[] args) throws Exception {
    CmdLineParser parser = new CmdLineParser(this);
    parseCmdLine(parser, args);

    if (_help || _args.isEmpty()) {
      printUsage(parser);
      return;
    }

    if (_registryBaseURI == null || _registryBaseURI.isEmpty()) {
      throw new IllegalArgumentException("Where is the Registry Server?" +
          " Please configure "+Registry.REGISTRY_URI_CONF);
    }

    switch (_command) {
    case addvh:
      if (_args.size() != 1) {
        printUsageAndExit(parser,
            "addvh requires a service URI as the only argument");
      }
      URI uri = new URI(_args.get(0));
      addVirtualHost(uri);
      getVirtualHost(uri);
      break;

    case getvh:
      if (_args.size() != 1) {
        printUsageAndExit(parser,
            "getvh requires a service URI as the only argument");
      }
      getVirtualHost(new URI(_args.get(0)));
      break;

    case delvh:
      if (_args.size() != 1) {
        printUsageAndExit(parser,
            "delvh requires a service URI as the only argument");
      }
      delVirtualHost(new URI(_args.get(0)));
      break;

    default:
      printUsageAndExit(parser);
    }
  }

  public static void main(String[] args) throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    String registryURI = conf.get(Registry.REGISTRY_URI_CONF);
    String registryProxy = conf.get(Registry.REGISTRY_PROXY_ADDRESS_CONF);
    RegistryClient rc = new RegistryClient(registryURI, registryProxy);
    try {
      rc.run(args);
    } finally {
      rc.stop();
    }
  }
}
