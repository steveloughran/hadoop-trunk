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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * ProxyServer will sit in between the end user and AppMaster
 * web interfaces. 
 */
public class RegistryServer extends CompositeService {

  /**
   * Priority of the ResourceManager shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final Log LOG = LogFactory.getLog(RegistryServer.class);
  
  private Registry registry = null;
  
  public RegistryServer() {
    super(RegistryServer.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    Configuration config = new YarnConfiguration(conf);
    try {
      doSecureLogin(conf);      
    } catch(IOException ie) {
      throw new YarnRuntimeException("Registry Server Failed to login", ie);
    }
    registry = new Registry();
    addService(registry);
    super.serviceInit(config);
  }

  /**
   * Log in as the Kerberose principal designated for the registry
   * @param conf the configuration holding this information in it.
   * @throws IOException on any error.
   */
  protected void doSecureLogin(Configuration conf) throws IOException {
    SecurityUtil.login(conf, YarnConfiguration.PROXY_KEYTAB,
        YarnConfiguration.PROXY_PRINCIPAL);
  }

  /**
   * Wait for service to finish.
   * (Normally, it runs forever.)
   */
  private void join() {
    registry.join();
  }

  public static void main(String[] args) {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(RegistryServer.class, args, LOG);
    try {
      RegistryServer registry = new RegistryServer();
      ShutdownHookManager.get().addShutdownHook(
        new CompositeServiceShutdownHook(registry),
        SHUTDOWN_HOOK_PRIORITY);
      YarnConfiguration conf = new YarnConfiguration();
      registry.init(conf);
      registry.start();
      registry.join();
    } catch (Throwable t) {
      LOG.fatal("Error starting Registry server", t);
      System.exit(-1);
    }
  }

}
