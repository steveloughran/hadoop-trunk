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

package org.apache.hadoop.yarn.registry.server.services;

import com.google.common.base.Preconditions;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.services.BindingInformation;
import org.apache.hadoop.yarn.registry.client.services.RegistryBindingSource;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * This is a small, localhost Zookeeper service instance that is contained
 * in a YARN service...it's been derived from Apache Twill.
 * 
 * It implements {@link RegistryBindingSource} and provides binding information,
 * <i>once started</i>. Until <code>start()</code> is called, the hostname & 
 * port may be undefined. Accordingly, the service raises an exception in this
 * condition.
 * 
 * If you wish to chain together a registry service with this one under
 * the same <code>CompositeService</code>, this service must be added
 * as a child first.
 *
 * It also sets the configuration parameter
 * {@link RegistryConstants#KEY_REGISTRY_ZK_QUORUM}
 * to its connection string. Any code with access to the service configuration
 * can view it.
 */
public class MicroZookeeperService
    extends AbstractService
    implements RegistryBindingSource, RegistryConstants {


  private static final Logger
      LOG = LoggerFactory.getLogger(MicroZookeeperService.class);

  private File dataDir;
  private int tickTime;
  private int port;

  private ServerCnxnFactory factory;
  private BindingInformation binding;

  public MicroZookeeperService(String name) {
    super(name);
  }

  public String getConnectionString() {
    Preconditions.checkState(factory != null, "service not started");
    InetSocketAddress addr = factory.getLocalAddress();
    return String.format("%s:%d", addr.getHostName(), addr.getPort());
  }
  
  public InetSocketAddress getConnectionAddress() {
    Preconditions.checkState(factory !=null, "service not started");
    return factory.getLocalAddress();
  }

  /**
   * Create an inet socket addr from the local host+ port number
   * @param port port to use 
   * @return a (hostname, port) pair
   * @throws UnknownHostException if the machine doesn't know its own address
   */
  private InetSocketAddress getAddress(int port) throws UnknownHostException {
    return new InetSocketAddress(InetAddress.getLocalHost(),
        port < 0 ? 0 : port);
  }

  /**
   * Initialize the service
   * @param conf configuration
   * @throws Exception
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    port = getConfig().getInt(KEY_ZKSERVICE_PORT, 0);
    tickTime = getConfig().getInt(KEY_ZKSERVICE_TICK_TIME,
        ZooKeeperServer.DEFAULT_TICK_TIME);
    String datapathname = getConfig().getTrimmed(KEY_ZKSERVICE_DATADIR, "");
    if (datapathname.isEmpty()) {
      dataDir = File.createTempFile("zkservice", ".dir");
      dataDir.delete();
    } else {
      dataDir = new File(datapathname);
      FileUtil.fullyDelete(dataDir);
    }
    LOG.debug("Data directory is {}", dataDir);
    // the exit code here is ambigious
    dataDir.mkdirs();
    // so: verify the path is there and a directory
    if (!dataDir.exists()) {
      throw new FileNotFoundException("failed to create directory " + dataDir);
    }
    if (!dataDir.isDirectory()) {
      
      throw new IOException("Path "+dataDir +" exists but is not a directory "
        + " isDir()=" + dataDir.isDirectory() 
        + " isFile()=" + dataDir.isFile());
    }
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    ZooKeeperServer zkServer = new ZooKeeperServer();
    FileTxnSnapLog ftxn = new FileTxnSnapLog(dataDir, dataDir);
    zkServer.setTxnLogFactory(ftxn);
    zkServer.setTickTime(tickTime);

    LOG.info("Starting Local Zookeeper service");
    factory = ServerCnxnFactory.createFactory();
    factory.configure(getAddress(port), -1);
    factory.startup(zkServer);

    String connectString = getConnectionString();
    LOG.info("In memory ZK started: {}", connectString);
    if (LOG.isDebugEnabled()) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      zkServer.dumpConf(pw);
      pw.flush();
      LOG.debug(sw.toString());
    }
    binding = new BindingInformation();
    binding.ensembleProvider = new FixedEnsembleProvider(connectString);
    binding.description =
        getName() + " reachable at \"" + connectString + "\"";
    
    // finally: set the binding information in the config
    getConfig().set(KEY_REGISTRY_ZK_QUORUM, connectString);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (factory != null) {
      factory.shutdown();
      factory = null;
    }
    if (dataDir != null) {
      FileUtil.fullyDelete(dataDir);
    }
  }

  @Override
  public BindingInformation supplyBindingInformation() {
    Preconditions.checkNotNull(binding,
        "Service is not started: binding information undefined");
    return binding;
  }
}
