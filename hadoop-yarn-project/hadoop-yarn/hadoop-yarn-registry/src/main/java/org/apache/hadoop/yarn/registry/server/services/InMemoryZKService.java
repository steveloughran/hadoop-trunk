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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * This is a Zookeeper service instance that is contained in a YARN
 * service...it's been derived from Apache Twill for testing purposes
 */
public class InMemoryZKService extends AbstractService {

  public static final String KEY_TICK_TIME = "zkservice.ticktime";
  public static final String KEY_PORT = "zkservice.port";
  public static final String KEY_DATADIR = "zkservice.datadir";
  private static final Logger
      LOG = LoggerFactory.getLogger(InMemoryZKService.class);

  private File dataDir;
  private int tickTime;
  private int port;

  private ServerCnxnFactory factory;

  public InMemoryZKService(String name) {
    super(name);
  }

  public String getConnectionString() {
    InetSocketAddress addr = factory.getLocalAddress();
    return String.format("%s:%d", addr.getHostName(), addr.getPort());
  }
  public InetSocketAddress getConnectionAddress() {
    return factory.getLocalAddress();
  }

  public InetSocketAddress getLocalAddress() {
    return factory.getLocalAddress();
  }

  private InetSocketAddress getAddress(int port) throws UnknownHostException {
    return new InetSocketAddress(InetAddress.getLocalHost(),
        port < 0 ? 0 : port);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    port = getConfig().getInt(KEY_PORT, 0);
    tickTime = getConfig().getInt(KEY_TICK_TIME,
        ZooKeeperServer.DEFAULT_TICK_TIME);
    dataDir = new File(getConfig().get(KEY_DATADIR, "target/zookeeper"));
  }

  @Override
  protected void serviceStart() throws Exception {
    ZooKeeperServer zkServer = new ZooKeeperServer();
    FileTxnSnapLog ftxn = new FileTxnSnapLog(dataDir, dataDir);
    zkServer.setTxnLogFactory(ftxn);
    zkServer.setTickTime(tickTime);

    LOG.info("Starting Zookeeper server");
    factory = ServerCnxnFactory.createFactory();
    factory.configure(getAddress(port), -1);
    factory.startup(zkServer);

    LOG.info("In memory ZK started: " + getConnectionString());
    if (LOG.isDebugEnabled()) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      zkServer.dumpConf(pw);
      pw.flush();
      LOG.debug(sw.toString());
    }

  }

  @Override
  protected void serviceStop() throws Exception {
    if (factory != null) {
      factory.shutdown();
    }
  }
}
