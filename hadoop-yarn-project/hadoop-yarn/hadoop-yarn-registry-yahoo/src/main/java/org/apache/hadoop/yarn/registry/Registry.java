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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.registry.webapp.WebApp;
import org.apache.hadoop.yarn.registry.security.SecurityData;
import org.apache.hadoop.yarn.registry.security.Owner;

public class Registry extends AbstractService {
  private static final Log LOG = LogFactory.getLog(Registry.class);

  //TODO move this into YarnConfiguration
  public static final String REGISTRY_URI_CONF = "yarn.registry.uri";
  public static final String REGISTRY_PROXY_ADDRESS_CONF = "yarn.registry.proxy.address";
  public static final String REGISTRY_STORAGE_CLASS_CONF = "yarn.registry.storage.class";
  public static final String REGISTRY_ENCRYPTOR_CLASS_CONF = "yarn.registry.encryptor.class";

  public static final String STORAGE_ATTRIBUTE = "yarn.registry.storage";
  public static final String ENCRYPTOR_ATTRIBUTE = "yarn.registry.encryptor";

  public static final String REGISTRY_HTTP_PLUGINS = "yarn.registry.plugins";
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
  private Storage storage = null;
  private Encryptor enc = null;
  private int port = 0;

  public Registry() {
    super(Registry.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    Class<? extends Storage> sc = conf.getClass(REGISTRY_STORAGE_CLASS_CONF, null, Storage.class);
    if (sc == null) {
        throw new IllegalArgumentException(REGISTRY_STORAGE_CLASS_CONF + " is not set");
    }
    storage = ReflectionUtils.newInstance(sc, conf);
    storage.init(conf);

    Class<? extends Encryptor> ec = conf.getClass(REGISTRY_ENCRYPTOR_CLASS_CONF, NoopEncryptor.class, Encryptor.class);
    enc = ReflectionUtils.newInstance(ec, conf);
    enc.init(conf);

    String uriStr = conf.get(REGISTRY_URI_CONF);
    if(uriStr == null || uriStr.isEmpty()) {
      throw new YarnRuntimeException( REGISTRY_URI_CONF +
          " is not set so the proxy will not run.");
    }
    URI uri = new URI(uriStr);
    bindAddress = uri.getHost();
    port = Math.max(0, uri.getPort()); // Use port of 0 when undefined
    LOG.info("Instantiating Registry at " + bindAddress + ":" + port);
    SecurityData.initCache(conf);
    Owner.initCache(conf);
    super.serviceInit(conf);
  }

  protected void addJerseyResources(HttpServer2 httpServer, Configuration conf) {
      String packages = null;
      String extPackages = conf.getTrimmed(REGISTRY_HTTP_PLUGINS);

      if (extPackages != null && !extPackages.isEmpty()) {
          packages = WebApp.class.getPackage().getName() + ";" + extPackages;
      } else {
          packages = WebApp.class.getPackage().getName();
      }

//      httpServer.addJerseyResourcePackageWithFilter(packages, "/registry/*" );
  }

  @Override
  protected void serviceStart() throws Exception {
    try {
      storage.start();
      enc.start();
      Configuration conf = getConfig();
      HttpServer2.Builder builder = new HttpServer2.Builder();
      builder.setName("registry")
             .setFindPort(port == 0)
             .addEndpoint(
                 new URI("http", null, bindAddress, port, "/", null, null))
          .setConf(conf);
      httpServer = builder.build();
      httpServer.setAttribute(STORAGE_ATTRIBUTE, storage);
      httpServer.setAttribute(ENCRYPTOR_ATTRIBUTE, enc);
      this.addJerseyResources(httpServer, conf);
      httpServer.start();
    } catch (IOException e) {
      LOG.fatal("Could not start registry web server",e);
      throw new YarnRuntimeException("Could not start registry web server",e);
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (storage != null) {
      storage.stop();
    }
    if (enc != null) {
      enc.stop();
    }
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

  public void join() {
    if(httpServer != null) {
      try {
        httpServer.join();
      } catch (InterruptedException e) {
      }
    }
  }
}
