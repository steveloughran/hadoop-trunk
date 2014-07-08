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
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ZKUtil;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.exceptions.ExceptionGenerator;
import org.apache.http.HttpStatus;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This implements the ZK binding
 */
public class RegistryZKService extends AbstractService
 implements RegistryConstants {
  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryZKService.class);
  private static final RetrySleeper sleeper = new RetrySleeper() {
    @Override
    public void sleepFor(long time, TimeUnit unit) throws InterruptedException {
      unit.sleep(time);
    }
  };
  public static final String PERMISSIONS_REGISTRY_ROOT = "world:anyone:rwcda";

  private CuratorFramework zk;
  private List<ACL> rootACL;


  /**
   * Construct the service.

   * @param name service name
   */
  public RegistryZKService(String name) {
    super(name);
  }


  public List<ACL> getRootACL() {
    return rootACL;
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();

    String root = getConfig().get(ZK_ROOT, REGISTRY_ROOT);

    rootACL = getACLs(ZK_ACL, PERMISSIONS_REGISTRY_ROOT);
    CuratorFramework rootCurator = newCurator("");
    try {
      if (rootCurator.checkExists().forPath(root) == null) {
        rootCurator.create().withACL(rootACL).forPath(root);
      }
    } finally {
      IOUtils.closeStream(rootCurator);
    }
    rootCurator.close();
    zk = newCurator(root);
  }

  /**
   * Close the ZK connection if it is open
   */
  @Override
  public void serviceStop() {
    IOUtils.closeStream(zk);
  }
  
  /**
   * Get the ACLs defined in the config key for this service, or
   * the default
   * @param confKey configuration key
   * @param defaultPermissions default values
   * @return an ACL list. 
   * @throws IOException
   * @throws ZKUtil.BadAclFormatException on a bad ACL parse
   */
  protected List<ACL> getACLs(String confKey, String defaultPermissions) throws
      IOException, ZKUtil.BadAclFormatException {
    String zkAclConf = getConfig().get(confKey, defaultPermissions);
    return parseACLs(zkAclConf);
  }

  /**
   * Parse an ACL list. This includes configuration indirection
   * {@link ZKUtil#resolveConfIndirection(String)}
   * @param zkAclConf configuration string
   * @return an ACL list
   * @throws IOException
   * @throws ZKUtil.BadAclFormatException on a bad ACL parse
   */
  protected  List<ACL> parseACLs(String zkAclConf) throws IOException,
      ZKUtil.BadAclFormatException {
    return ZKUtil.parseACLs(ZKUtil.resolveConfIndirection(zkAclConf));
  }

  private List<ACL> createAclForUser(String username) {
    return rootACL;
  }

 

  /**
   * Create a new curator instance off the root path
   * @param root
   * @return the newly created creator
   */
  private CuratorFramework newCurator(String root) {
    Configuration conf = getConfig();
    EnsembleProvider ensembleProvider = createEnsembleProvider(conf, root);
    int sessionTimeout = conf.getInt(ZK_SESSION_TIMEOUT,
        DEFAULT_ZK_SESSION_TIMEOUT);
    int connectionTimeout = conf.getInt(ZK_CONNECTION_TIMEOUT,
        DEFAULT_ZK_CONNECTION_TIMEOUT);
    int retryTimes = conf.getInt(ZK_RETRY_TIMES, DEFAULT_ZK_RETRY_TIMES);
    int retryInterval = conf.getInt(ZK_RETRY_INTERVAL,
        DEFAULT_ZK_RETRY_INTERVAL);
    int retryCeiling = conf.getInt(ZK_RETRY_CEILING, DEFAULT_ZK_RETRY_CEILING);

    CuratorFrameworkFactory.Builder b = CuratorFrameworkFactory.builder();
    b.ensembleProvider(ensembleProvider)
     .connectionTimeoutMs(connectionTimeout)
     .sessionTimeoutMs(sessionTimeout)
     .retryPolicy(new BoundedExponentialBackoffRetry(retryInterval,
         retryTimes,
         retryCeiling));

    CuratorFramework framework = b.build();
    framework.start();

    return framework;
  }

  /**
   * Create the ensemble provider for this registry. 
   * The initial implementation returns a fixed ensemble; an
   * Exhibitor-bonded ensemble provider is a future enhancement
   * @param conf configuration
   * @param root root path
   * @return the ensemble provider for this ZK service
   */
  private EnsembleProvider createEnsembleProvider(Configuration conf,
      String root) {
    String connectString = conf.get(ZK_HOSTS, DEFAULT_ZK_HOSTS) + root;
    LOG.debug("ZK connection is fixed at {}", connectString);
    return new FixedEnsembleProvider(connectString);
  }

  /**
   * Create an IOE when an operation fails
   * @param path path of operation
   * @param operation operation attempted
   * @param exception caught the exception caught
   * @return an IOE to throw that contains the path and operation details.
   */
  private IOException operationFailure(String path,
      String operation,
      Exception exception) {
    return ExceptionGenerator.generate(
        HttpStatus.SC_INTERNAL_SERVER_ERROR,
        path,
        "Failure of " + operation + " on " + path,
        exception);
  }

  /**
   * Create a path if it does not exist. 
   * The check is poll + create; there's a risk that another process
   * may create the same path before the create() operation is executed/
   * propagated to the ZK node polled.
   * 
   * @param path path to create
   * @return true iff the path was created
   * @throws IOException
   */
  public boolean maybeCreate(String path, CreateMode mode) throws IOException {
    List<ACL> acl = rootACL;
    return maybeCreate(path, mode, acl);
  }

  /**
   * Create a path if it does not exist. 
   * The check is poll + create; there's a risk that another process
   * may create the same path before the create() operation is executed/
   * propagated to the ZK node polled.
   *
   * @param path path to create
   * @param acl ACL for path -used when creating a new entry
   * @return true iff the path was created
   * @throws IOException
   */
  public boolean maybeCreate(String path,
      CreateMode mode,
      List<ACL> acl) throws IOException {
    if (!exists(path)) {
      mkdir(path, mode, acl);
      return true;
    }
    return false;
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
      LOG.debug("Exists({})", path);

      return zk.checkExists().forPath(path) != null;
    } catch (Exception e) {
      throw operationFailure(path, "existence check", e);
    }
  }

  protected boolean existsRobust(String path) {
    try {
      return exists(path);
    } catch (IOException e) {
      return false;
    }
  }
  
  /**
   * Verify a path exists
   * @param path path of operation
   * @throws FileNotFoundException if the path is absent
   * @throws IOException
   */
  public void verifyExists(String path) throws IOException {
    if (!exists(path)) {
      throw new FileNotFoundException(path);
    }
  }
  /**
   * Create a directory
   * @param path path to create
   * @param mode mode for path
   * @throws IOException
   */
  public void mkdir(String path, CreateMode mode) throws IOException {
    mkdir(path, mode, rootACL);
  }

  /**
   * Create a directory
   * @param path path to create
   * @param mode mode for path
   * @param acl ACL for path
   * @throws IOException
   */
  protected void mkdir(String path, CreateMode mode, List<ACL> acl) throws
      IOException {
    try {
      zk.create().withMode(mode).withACL(acl).forPath(path);
      LOG.debug("Created path {} with mode {} and ACL {}", path, mode, acl );
    } catch (Exception e) {
      throw operationFailure(path, "mkdir() ", e);
    }
  }

  /**
   * Create a path with given data. byte[0] is used for a path
   * without data
   * @param path path of operation
   * @param data initial data
   * @param acl
   * @throws IOException
   */
  public void create(String path,
      CreateMode mode,
      byte[] data,
      List<ACL> acl) throws IOException {
    try {
      LOG.debug("Creating {} with {} bytes", path, data.length);
      zk.create().withMode(mode).withACL(acl).forPath(path, data);
    } catch (Exception e) {
      if (existsRobust(path)) {
        throw new FileAlreadyExistsException(path);
      }
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
      LOG.debug("Updating {} with {} bytes", path, data.length);
      zk.setData().forPath(path, data);
    } catch (Exception e) {
      if (!existsRobust(path)) {
        throw new FileNotFoundException(path);
      }
      throw operationFailure(path, "update()", e);
    }
  }

  /**
   * Create or update an entry
   * @param path path
   * @param data data
   * @param acl ACL for path -used when creating a new entry
   * @throws IOException
   */
  public void set(String path, CreateMode mode, byte[] data, List<ACL> acl) throws IOException {
    if (!exists(path)) {
      create(path, mode, data, acl);
    } else {
      update(path, data);
    }
  }

  /**
   * Delete a directory/directory tree.
   * It is not an error to delete a path that does not exist
   * @param path path of operation
   * @param recursive flag to trigger recursive deletion
   * @throws IOException
   */
  public void rm(String path, boolean recursive) throws IOException {
    try {
      LOG.debug("Deleting {}", path);
      DeleteBuilder delete = zk.delete();
      if (recursive) {
        delete.deletingChildrenIfNeeded();
      }
      delete.forPath(path);
    } catch (Exception e) {
      if (!existsRobust(path)) {
        // not an error
        return;
      }
      throw operationFailure(path, "delete()", e);
    }
  }
  public List<String> ls(String path) throws IOException {
    try {
      LOG.debug("ls {}", path);
      GetChildrenBuilder builder = zk.getChildren();
      List<String> children = builder.forPath(path);
      return children;

    } catch (Exception e) {
      IOException ioe = operationFailure(path, "ls()", e);
      if (!existsRobust(path)) {
        ioe = new FileNotFoundException(path);
      }
      throw ioe;
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
      LOG.debug("Reading {}", path);
      return zk.getData().forPath(path);
    } catch (KeeperException.NoNodeException e) {
      return null;
    } catch (Exception e) {
      throw operationFailure(path, "read() " + path, e);
    }
  }

}
