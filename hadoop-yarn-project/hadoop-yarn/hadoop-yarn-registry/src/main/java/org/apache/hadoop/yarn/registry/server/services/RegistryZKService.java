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

import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ZKUtil;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.exceptions.ExceptionGenerator;
import org.apache.http.HttpStatus;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This implements the ZK binding
 */
public class RegistryZKService extends AbstractService
 implements RegistryConstants {

  private static final RetrySleeper sleeper = new RetrySleeper() {
    @Override
    public void sleepFor(long time, TimeUnit unit) throws InterruptedException {
      unit.sleep(time);
    }
  };

  private CuratorFramework zk;
  private List<ACL> zkAcl;
  private BoundedExponentialBackoffRetry retry;


  /**
   * Construct the service.

   * @param name service name
   */
  public RegistryZKService(String name) {
    super(name);
  }


  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    Configuration conf = getConfig();

    retry = new BoundedExponentialBackoffRetry(10, 100, 10); //Don't hammer ZK

    String root = conf.get(ZK_ROOT, "/yarnRegistry");

    String zkAclConf = conf.get(ZK_ACL, "world:anyone:rwcda");
    zkAclConf = ZKUtil.resolveConfIndirection(zkAclConf);
    zkAcl = ZKUtil.parseACLs(zkAclConf);
    CuratorFramework tmp = newCurator("");
    if (tmp.checkExists().forPath(root) == null) {
      tmp.create().withACL(zkAcl).forPath(root);
    }
    tmp.close();
    zk = newCurator(root);
    maybeCreate("/vh", CreateMode.PERSISTENT);
  }

  /**
   * Close the ZK connection if it is open
   */
  @Override
  public void serviceStop() {
    IOUtils.closeStream(zk);
  }

  /**
   * Create a new curator instance
   * @param root
   * @return
   */
  private CuratorFramework newCurator(String root) {
    Configuration conf = getConfig();
    String connectString = conf.get(ZK_HOSTS, DEFAULT_ZK_HOSTS) + root;
    int sessionTimeout = conf.getInt(ZK_SESSION_TIMEOUT,
        DEFAULT_ZK_SESSION_TIMEOUT);
    int connectionTimeout = conf.getInt(ZK_CONNECTION_TIMEOUT,
        DEFAULT_ZK_CONNECTION_TIMEOUT);
    int retryTimes = conf.getInt(ZK_RETRY_TIMES, DEFAULT_ZK_RETRY_TIMES);
    int retryInterval = conf.getInt(ZK_RETRY_INTERVAL,
        DEFAULT_ZK_RETRY_INTERVAL);
    int retryCeiling = conf.getInt(ZK_RETRY_CEILING, DEFAULT_ZK_RETRY_CEILING);

    CuratorFrameworkFactory.Builder b = CuratorFrameworkFactory.builder();
    b.connectString(connectString)
     .connectionTimeoutMs(connectionTimeout)
     .sessionTimeoutMs(sessionTimeout)
     .retryPolicy(new BoundedExponentialBackoffRetry(retryInterval, retryTimes,
         retryCeiling));

    CuratorFramework framework = b.build();
    framework.start();

    return framework;
  }

  /**
   * Create an IOE when an operation fails
   * @param path path of operation
   * @param operation operation attempted
   * @param exception caught
   * @return an IOE to throw that contains the path and operation details.
   */
  private IOException operationFailure(String path,
      String operation,
      Exception e) {
    return ExceptionGenerator.generate(
        HttpStatus.SC_INTERNAL_SERVER_ERROR,
        path,
        "Failure of " + operation + " on " + path,
        e);
  }

  /**
   * Create a path if it does not exist. 
   * The check is poll + create; there's a risk that another process
   * may create the same path before the create() operation is executed/
   * propagated to the ZK node polled.
   * @param path path to create
   * @throws IOException
   */
  public void maybeCreate(String path, CreateMode mode) throws IOException {
    if (!exists(path)) {
      mkdir(path, mode);
    }
  }

  /**
   * Poll for a path existing
   * @param path path of operation
   * @return true if the path was visible from the ZK server
   * queried.
   * @throws IOException
   */
  public boolean exists(String path) throws IOException {
    try {
      return zk.checkExists().forPath(path) != null;
    } catch (Exception e) {
      throw operationFailure(path, "existence check", e);
    }
  }

  /**
   * Create a directory
   * @param path path to create
   * @throws IOException
   */
  public void mkdir(String path, CreateMode mode) throws IOException {
    try {
      zk.create().withMode(mode).withACL(zkAcl).forPath(path);
    } catch (Exception e) {
      throw operationFailure(path, "mkdir() ", e);
    }
  }

  /**
   * Create a path with given data. byte[0] is used for a path
   * without data
   * @param path path of operation
   * @param data initial data
   * @throws IOException
   */
  public void create(String path, CreateMode mode,  byte[] data) throws IOException {
    try {
      zk.create().withMode(mode).withACL(zkAcl).forPath(path, data);
    } catch (Exception e) {
      throw operationFailure(path, "create()", e);
    }
  }

  /**
   * Update the data for a path
   * @param path path of operation
   * @param data new data
   * @throws IOException
   */
  public void update(String path, byte[] data) throws IOException {
    try {
      zk.setData().forPath(path, data);
    } catch (Exception e) {
      throw operationFailure(path, "update()", e);
    }
  }

  /**
   * Create or update an entry
   * @param path path
   * @param data data
   * @throws IOException
   */
  public void set(String path, CreateMode mode,  byte[] data) throws IOException {
    if (!exists(path)) {
      create(path, mode, data);
    } else {
      update(path, data);
    }
  }

  /**
   * Read data on a path
   * @param path path of operation
   * @return the data
   * @throws IOException read failure
   */
  public byte[] read(String path) throws IOException {
    try {
      return zk.getData().forPath(path);
    } catch (KeeperException.NoNodeException e) {
      return null;
    } catch (Exception e) {
      throw operationFailure(path, "read() " + path, e);
    }
  }

}
