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


package org.apache.hadoop.yarn.registry.server.web;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;

import java.net.URI;

public class RegistryWebService extends CompositeService
implements RegistryConstants {
  private static final Log LOG = LogFactory.getLog(RegistryWebService.class);
  
/*

  public static final class HttpServer2 extends HttpServer2 {
    public HttpServer2(String name, String bindAddress, int port,
        boolean findPort, Configuration conf) throws IOException {
      super(name, bindAddress, port, findPort, conf, null, null, null);
    }

    //TODO move this into HttpServer itself
    public void addJerseyResourcePackageWithFilter(final String packageName,
        final String pathSpec) {
      addJerseyResourcePackage(packageName, pathSpec);
      addFilterPathMapping(pathSpec, webAppContext);
    }
  }
*/

  private HttpServer2 httpServer = null;
  private String bindAddress = null;
  private int port = 0;

  public RegistryWebService() {
    super(RegistryWebService.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    String uriStr = conf.get(REGISTRY_URI_CONF);
    if(uriStr == null || uriStr.isEmpty()) {
      throw new YarnRuntimeException( REGISTRY_URI_CONF +
          " is not set so the proxy will not run.");
    }
    URI uri = new URI(uriStr);
    bindAddress = uri.getHost();
    port = Math.max(0, uri.getPort()); // Use port of 0 when undefined
    LOG.info("Instantiating Registry at " + bindAddress + ":" + port);
    super.serviceInit(conf);
  }

  protected void addJerseyResources(HttpServer2 httpServer, Configuration conf) {
/*
      String packages = null;
      String extPackages = conf.getTrimmed(REGISTRY_HTTP_PLUGINS);

      if (extPackages != null && !extPackages.isEmpty()) {
          packages = WebApp.class.getPackage().getName() + ";" + extPackages;
      } else {
          packages = WebApp.class.getPackage().getName();
      }
*/

//      httpServer.addJerseyResourcePackageWithFilter(packages, "/registry/*" );
  }

  @Override
  protected void serviceStart() throws Exception {
      Configuration conf = getConfig();
      HttpServer2.Builder builder = new HttpServer2.Builder();
      builder.setName("registry")
             .setFindPort(port == 0)
             .addEndpoint(
                 new URI("http", null, bindAddress, port, "/", null, null))
          .setConf(conf);
      httpServer = builder.build();
      this.addJerseyResources(httpServer, conf);
      httpServer.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (httpServer != null) {
      try {
        httpServer.stop();
      } catch (Exception e) {
        LOG.fatal("Error stopping registry web server", e);
        throw new YarnRuntimeException("Error stopping registry web server",e);
      }
    }
    super.serviceStop();
  }

  /**
   * Run a service. This called after {@link Service#start()}
   * @return the exit code
   * @throws Throwable any exception to report
   */
  int runService() throws Throwable {
    if(httpServer != null) {
      try {
        httpServer.join();
      } catch (InterruptedException e) {
        // interrupted
        return 0;
      }
    }
    return 0;
  }
}