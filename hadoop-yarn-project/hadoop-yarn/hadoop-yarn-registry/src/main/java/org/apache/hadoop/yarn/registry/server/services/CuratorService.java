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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ZKUtil;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.binding.RegistryZKUtils;
import org.apache.hadoop.yarn.registry.client.binding.ZKPathDumper;
import org.apache.hadoop.yarn.registry.client.exceptions.ExceptionGenerator;
import org.apache.hadoop.yarn.registry.client.exceptions.NoChildrenForEphemeralsException;
import org.apache.http.HttpStatus;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This service binds to Zookeeper via Apache Curator
 */
public class CuratorService extends AbstractService
    implements RegistryConstants {
  public static final String PERMISSIONS_REGISTRY_ROOT = "world:anyone:rwcda";
  private static final Logger LOG =
      LoggerFactory.getLogger(CuratorService.class);
  private static final RetrySleeper sleeper = new RetrySleeper() {
    @Override
    public void sleepFor(long time, TimeUnit unit) throws InterruptedException {
      unit.sleep(time);
    }
  };
  private CuratorFramework curator;
  private List<ACL> rootACL;
  private String registryRoot;
  /**
   * set the connection parameters
   */
  private String connectionParameterDescription;
  private String zookeeperQuorum;


  /**
   * Construct the service.

   * @param name service name
   */
  public CuratorService(String name) {
    super(name);
  }


  public List<ACL> getRootACL() {
    return rootACL;
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();

    registryRoot = getConfig().getTrimmed(REGISTRY_ZK_ROOT,
        DEFAULT_REGISTRY_ROOT) ;
    LOG.debug("Creating Registry with root {}", registryRoot);

    rootACL = getACLs(REGISTRY_ZK_ACL, PERMISSIONS_REGISTRY_ROOT);
/*
    CuratorFramework rootCurator = newCurator("");
    try {
      if (rootCurator.checkExists().forPath(root) == null) {
        rootCurator.create().withACL(rootACL).forPath(root);
      }
    } finally {
      IOUtils.closeStream(rootCurator);
    }
    rootCurator.close();
*/
//    zk = newCurator(root);
    curator = newCurator();
    maybeCreate("", CreateMode.PERSISTENT, rootACL);
  }

  /**
   * Close the ZK connection if it is open
   */
  @Override
  public void serviceStop() throws Exception {
    IOUtils.closeStream(curator);
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
  protected List<ACL> parseACLs(String zkAclConf) throws IOException,
      ZKUtil.BadAclFormatException {
    return ZKUtil.parseACLs(ZKUtil.resolveConfIndirection(zkAclConf));
  }

  private List<ACL> createAclForUser(String username) {
    return rootACL;
  }


  /**
   * Create a new curator instance off the root path; using configuration
   * options provided in the service configuration to set timeouts and
   * retry policy.
   * @return the newly created creator
   */
  private CuratorFramework newCurator() {
    Configuration conf = getConfig();
    EnsembleProvider ensembleProvider = createEnsembleProvider();
    int sessionTimeout = conf.getInt(REGISTRY_ZK_SESSION_TIMEOUT,
        DEFAULT_ZK_SESSION_TIMEOUT);
    int connectionTimeout = conf.getInt(REGISTRY_ZK_CONNECTION_TIMEOUT,
        DEFAULT_ZK_CONNECTION_TIMEOUT);
    int retryTimes = conf.getInt(REGISTRY_ZK_RETRY_TIMES, DEFAULT_ZK_RETRY_TIMES);
    int retryInterval = conf.getInt(REGISTRY_ZK_RETRY_INTERVAL,
        DEFAULT_ZK_RETRY_INTERVAL);
    int retryCeiling = conf.getInt(REGISTRY_ZK_RETRY_CEILING, DEFAULT_ZK_RETRY_CEILING);

    LOG.debug("Creating CuratorService with connection {}",
        connectionParameterDescription);
    CuratorFrameworkFactory.Builder b = CuratorFrameworkFactory.builder();
    b.ensembleProvider(ensembleProvider)
     .connectionTimeoutMs(connectionTimeout)
     .sessionTimeoutMs(sessionTimeout)
     .retryPolicy(new BoundedExponentialBackoffRetry(retryInterval,
         retryTimes,
         retryCeiling));


/*
    if (!root.isEmpty()) {
      String namespace = root;
      if (namespace.startsWith("/")) {
        namespace = namespace.substring(1);
      }
      b.namespace(namespace);
    }
*/

    CuratorFramework framework = b.build();
    framework.start();

    return framework;
  }

  @Override
  public String toString() {
    return super.toString()
           + " ZK quorum=\"" + getCurrentZookeeperQuorum() +"\""
           + " root=\"" + registryRoot + "\"";

  }


  public String getCurrentZookeeperQuorum() {
    return zookeeperQuorum;
  }

  /*
       * Create a full path from the registry root and the supplied subdir
       * @param path path of operation
       * @return an absolute path
       * @throws IllegalArgumentException if the path is invalide
       */
  protected String createFullPath(String path) throws IOException {
    return RegistryZKUtils.createFullPath(registryRoot, path);
  }

  /**
   * Create the ensemble provider for this registry. 
   * The initial implementation returns a fixed ensemble; an
   * Exhibitor-bonded ensemble provider is a future enhancement.
   * 
   * This sets {@link #connectionParameterDescription} to the binding info
   * for use in toString and logging;
   * 
   * @return the ensemble provider for this ZK service
   */
  private EnsembleProvider createEnsembleProvider() {
    String connectString = getConfig().getTrimmed(REGISTRY_ZK_QUORUM,
        DEFAULT_ZK_HOSTS);
    connectionParameterDescription =
        "fixed ZK quorum \"" + connectString + "\"";
    zookeeperQuorum = connectString;
    return new FixedEnsembleProvider(connectString);
  }

  /**
   * Create an IOE when an operation fails
   * @param path path of operation
   * @param operation operation attempted
   * @param exception caught the exception caught
   * @return an IOE to throw that contains the path and operation details.
   */
  protected IOException operationFailure(String path,
      String operation,
      Exception exception) {
    IOException ioe;
    if (exception instanceof KeeperException.NoNodeException) {
      ioe = new FileNotFoundException(path);
    } else if (exception instanceof KeeperException.NodeExistsException) {
      ioe = new FileAlreadyExistsException(path);
      
    } else if (exception instanceof KeeperException.NotEmptyException) {
      ioe = new PathIsNotEmptyDirectoryException(path);
      
    } else if (exception instanceof KeeperException.NoChildrenForEphemeralsException) {
      ioe = new NoChildrenForEphemeralsException(path + ": " + exception,
          exception);
    } else {
      ioe = ExceptionGenerator.generate(
          HttpStatus.SC_INTERNAL_SERVER_ERROR,
          path,
          "Failure of " + operation + " on " + path + ": " +
          exception.toString(),
          exception);
    }
    if (ioe.getCause() == null) {
      ioe.initCause(exception);
    }

    return ioe;
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
    if (!zkPathExists(path)) {
      zkMkPath(path, mode, acl);
      return true;
    }
    return false;
  }


  /**
   * Stat the file
   * @param path
   * @return
   * @throws IOException
   */
  public Stat zkStat(String path) throws IOException {
    String fullpath = createFullPath(path);
    try {
      LOG.debug("Stat {}", fullpath);
      return curator.checkExists().forPath(fullpath);
    } catch (Exception e) {
      throw operationFailure(fullpath, "read()", e);
    }
  }
  
  /**
   * Poll for a path existing
   * @param path path of operation
   * @return true if the path was visible from the ZK server
   * queried.
   * @throws IOException
   */
  public boolean zkPathExists(String path) throws IOException {
    try {
      return zkStat(path) != null;

    } catch (FileNotFoundException e) {
      return false;
    } catch (Exception e) {
      throw operationFailure(path, "existence check", e);
    }
  }

  protected boolean pathExistsRobust(String path) {
    try {
      return zkPathExists(path);
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
  public String zkPathMustExist(String path) throws IOException {
    if (!zkPathExists(path)) {
      throw new FileNotFoundException(path);
    }
    return path;
  }

  /**
   * Create a path
   * @param path path to create
   * @param mode mode for path
   * @throws IOException
   */
  public void zkMkpath(String path, CreateMode mode) throws IOException {
    zkMkPath(path, mode, rootACL);
  }

  /**
   * Create a directory
   * @param path path to create
   * @param mode mode for path
   * @param acl ACL for path
   * @throws IOException
   */
  protected void zkMkPath(String path, CreateMode mode, List<ACL> acl) throws
      IOException {
    path = createFullPath(path);
    try {
      curator.create().withMode(mode).withACL(acl).forPath(path);
      LOG.debug("Created path {} with mode {} and ACL {}", path, mode, acl);
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
  public void zkCreate(String path,
      CreateMode mode,
      byte[] data,
      List<ACL> acl) throws IOException {
    String fullpath = createFullPath(path);
    try {
      LOG.debug("Creating {} with {} bytes", fullpath, data.length);
      curator.create().withMode(mode).withACL(acl).forPath(fullpath, data);
    } catch (Exception e) {
      throw operationFailure(fullpath, "create()", e);
    }
  }

  /**
   * Update the data for a path
   * @param path path of operation
   * @param data new data
   * @throws IOException
   */
  public void zkUpdate(String path, byte[] data) throws IOException {
    path = createFullPath(path);
    try {
      LOG.debug("Updating {} with {} bytes", path, data.length);
      curator.setData().forPath(path, data);
    } catch (Exception e) {
      throw operationFailure(path, "update()", e);
    }
  }

  /**
   * Create or update an entry
   * @param path path
   * @param data data
   * @param acl ACL for path -used when creating a new entry
   * @throws IOException
   * @return true if the entry was created, false if it was simply updated.
   */
  public boolean zkSet(String path,
      CreateMode mode,
      byte[] data,
      List<ACL> acl) throws IOException {
    if (!zkPathExists(path)) {
      zkCreate(path, mode, data, acl);
      return true;
    } else {
      zkUpdate(path, data);
      return false;
    }
  }

  /**
   * Delete a directory/directory tree.
   * It is not an error to delete a path that does not exist
   * @param path path of operation
   * @param recursive flag to trigger recursive deletion
   * @throws IOException
   */
  public void zkDelete(String path, boolean recursive) throws IOException {
    String fullpath = createFullPath(path);
    try {
      LOG.debug("Deleting {}", fullpath);
      DeleteBuilder delete = curator.delete();
      if (recursive) {
        delete.deletingChildrenIfNeeded();
      }
      delete.forPath(fullpath);
    } catch (KeeperException.NoNodeException e) {
      // not an error
    } catch (Exception e) {
      throw operationFailure(fullpath, "delete()", e);
    }
  }

  public List<String> zkList(String path) throws IOException {
    String fullpath = createFullPath(path);
    try {
      LOG.debug("ls {}", fullpath);
      GetChildrenBuilder builder = curator.getChildren();
      List<String> children = builder.forPath(fullpath);
      return children;

    } catch (Exception e) {
      throw operationFailure(path, "ls()", e);
    }
  }

  /**
   * Read data on a path
   * @param path path of operation
   * @return the data
   * @throws IOException read failure
   */
  public byte[] zkRead(String path) throws IOException {
    String fullpath = createFullPath(path);
    try {
      LOG.debug("Reading {}", fullpath);
      return curator.getData().forPath(fullpath);
    } catch (Exception e) {
      throw operationFailure(fullpath, "read()", e);
    }
  }


  /**
   * List the children of a path
   * @param path path of operation
   * @return the children -or an empty list if the path does not exist
   * @throws IOException read failure
   */
  public List<String> listChildren(String path) throws IOException {
    if (!zkPathExists(path)) {
      return Collections.emptyList();
    }
    return zkList(path);
  }
  
  @VisibleForTesting
  public ZKPathDumper dumpPath(boolean verbose) {
    return new ZKPathDumper(curator, registryRoot, verbose);
  }
}
