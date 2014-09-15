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

package org.apache.hadoop.yarn.registry.client.services.zk;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathAccessDeniedException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ZKUtil;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.yarn.registry.client.binding.ZKPathDumper;
import org.apache.hadoop.yarn.registry.client.exceptions.AuthenticationFailedException;
import org.apache.hadoop.yarn.registry.client.exceptions.NoChildrenForEphemeralsException;
import org.apache.hadoop.yarn.registry.client.exceptions.RegistryIOException;
import org.apache.hadoop.yarn.registry.client.services.BindingInformation;
import org.apache.hadoop.yarn.registry.client.services.RegistryBindingSource;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * This service binds to Zookeeper via Apache Curator. It is more 
 * generic than just the YARN service registry; it does not implement
 * any of the RegistryOperations API. 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CuratorService extends CompositeService
    implements RegistryConstants, RegistryBindingSource {

  private static final Logger LOG =
      LoggerFactory.getLogger(CuratorService.class);
  public static final String SASL = "sasl";

  /**
   * the Curator binding
   */
  private CuratorFramework curator;

  /**
   * Parsed root ACL
   */
  private List<ACL> rootACL;

  /**
   * Path to the registry root
   */
  private String registryRoot;

  private final RegistryBindingSource bindingSource;

  /**
   * the connection binding text for messages
   */
  private String connectionDescription;
  private String securityConnectionDiagnostics = "";
  
  private EnsembleProvider ensembleProvider;


  /**
   * Construct the service.
   * @param name service name
   * @param bindingSource source of binding information.
   * If null: use this instance
   */
  public CuratorService(String name, RegistryBindingSource bindingSource) {
    super(name);
    if (bindingSource != null) {
      this.bindingSource = bindingSource;
    } else {
      this.bindingSource = this;     
    }
  }

  /**
   * Create an instance using this service as the binding source (i.e. read
   * configuration options from the registry)
   * @param name service name
   */
  public CuratorService(String name) {
    this(name, null);
  }

  @Override
  protected void serviceStart() throws Exception {
 

    registryRoot = getConfig().getTrimmed(KEY_REGISTRY_ZK_ROOT,
        DEFAULT_REGISTRY_ROOT) ;
    LOG.debug("Creating Registry with root {}", registryRoot);

    rootACL = buildRootACL();
    curator = newCurator();
    super.serviceStart();
  }

  /**
   * Close the ZK connection if it is open
   */
  @Override
  protected void serviceStop() throws Exception {
    IOUtils.closeStream(curator);
    super.serviceStop();
  }


  /**
   * Override point: build the root acl
   *
   * @return the ACL
   * @throws IOException any bind/parse problem
   */
  protected List<ACL> buildRootACL() throws IOException {
    return buildACLs(KEY_REGISTRY_ZK_ACL, DEFAULT_REGISTRY_ROOT_PERMISSIONS);
  }

  protected List<ACL> getRootACL() {
    return rootACL;
  }

  public void setRootACL(List<ACL> rootACL) {
    this.rootACL = rootACL;
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
  public List<ACL> buildACLs(String confKey, String defaultPermissions) throws
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
  public List<ACL> parseACLs(String zkAclConf) throws IOException,
      ZKUtil.BadAclFormatException {
    return ZKUtil.parseACLs(ZKUtil.resolveConfIndirection(zkAclConf));
  }

  /**
   * Create a new curator instance off the root path; using configuration
   * options provided in the service configuration to set timeouts and
   * retry policy.
   * @return the newly created creator
   */
  private CuratorFramework newCurator() throws IOException {
    Configuration conf = getConfig();
    createEnsembleProvider();
    int sessionTimeout = conf.getInt(KEY_REGISTRY_ZK_SESSION_TIMEOUT,
        DEFAULT_ZK_SESSION_TIMEOUT);
    int connectionTimeout = conf.getInt(KEY_REGISTRY_ZK_CONNECTION_TIMEOUT,
        DEFAULT_ZK_CONNECTION_TIMEOUT);
    int retryTimes = conf.getInt(KEY_REGISTRY_ZK_RETRY_TIMES,
        DEFAULT_ZK_RETRY_TIMES);
    int retryInterval = conf.getInt(KEY_REGISTRY_ZK_RETRY_INTERVAL,
        DEFAULT_ZK_RETRY_INTERVAL);
    int retryCeiling = conf.getInt(KEY_REGISTRY_ZK_RETRY_CEILING,
        DEFAULT_ZK_RETRY_CEILING);

    LOG.debug("Creating CuratorService with connection {}",
        connectionDescription);
    CuratorFrameworkFactory.Builder b = CuratorFrameworkFactory.builder();
    b.ensembleProvider(ensembleProvider)
     .connectionTimeoutMs(connectionTimeout)
     .sessionTimeoutMs(sessionTimeout)
     .retryPolicy(new BoundedExponentialBackoffRetry(retryInterval,
         retryTimes,
         retryCeiling));

    addSecurityBinding(b);
       

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


  public CuratorFrameworkFactory.Builder addSecurityBinding(
      CuratorFrameworkFactory.Builder builder) throws
      IOException {
    Configuration conf = getConfig();
    RegistrySecurity security = new RegistrySecurity(conf);

    String principal = conf.get(KEY_REGISTRY_ZK_PRINCIPAL);


    if (StringUtils.isEmpty(principal)) {
      LOG.debug("No security principal...client is unauthed");
      return builder;
    }
    String zkKeytab = conf.getTrimmed(KEY_REGISTRY_ZK_KEYTAB);
    File keytabFile = new File(zkKeytab);
    File jaasFile =
        security.prepareJAASAuth(principal, keytabFile,
            File.createTempFile("curator-", ".jaas"));
    String authScheme = SASL;
    securityConnectionDiagnostics =
        String.format(
            " Secured as \"%s:%s\" keytab=%s jaasfile=%s",
            authScheme, principal, keytabFile, jaasFile);
    LOG.debug(securityConnectionDiagnostics);
    byte[] data = principal.getBytes("UTF-8");

    builder.authorization(authScheme, data);
    return builder;
  }
  
  @Override
  public String toString() {
    return super.toString()
           + bindingDiagnosticDetails();

  }

  public String bindingDiagnosticDetails() {
    return " Connection=\"" + connectionDescription + "\""
           + " root=\"" + registryRoot + "\""
           + " "+ securityConnectionDiagnostics;
  }

  /**
   * Create a full path from the registry root and the supplied subdir
   * @param path path of operation
   * @return an absolute path
   * @throws IllegalArgumentException if the path is invalide
   */
  protected String createFullPath(String path) throws IOException {
    return RegistryPathUtils.createFullPath(registryRoot, path);
  }

  /**
   * Get the registry binding source ... this can be used to 
   * create new ensemble providers
   * @return the registry binding source in use
   */
  public RegistryBindingSource getBindingSource() {
    return bindingSource;
  }

  /**
   * Create the ensemble provider for this registry, by invoking
   * {@link RegistryBindingSource#supplyBindingInformation()} on
   * the provider stored in {@link #bindingSource}
   * Sets {@link #ensembleProvider} to that value;
   * sets {@link #connectionDescription} to the binding info
   * for use in toString and logging;
   * 
   */
  protected void createEnsembleProvider() {
    BindingInformation binding = bindingSource.supplyBindingInformation();
    String connectString = buildConnectionString();
    connectionDescription = binding.description
                            + " " + securityConnectionDiagnostics;
    ensembleProvider = binding.ensembleProvider;
  }


  /**
   * Supply the binding information.
   * This implementation returns a fixed ensemble bonded to
   * the quorum supplied by {@link #buildConnectionString()}
   * @return
   */
  @Override
  public BindingInformation supplyBindingInformation() {
    BindingInformation binding = new BindingInformation();
    String connectString = buildConnectionString();
    binding.ensembleProvider = new FixedEnsembleProvider(connectString);
    binding.description =
        "fixed ZK quorum \"" + connectString + "\"";
    return binding;
  }
  
  /**
   * Override point: get the connection string used to connect to
   * the ZK service
   * @return a registry quorum
   */
  protected String buildConnectionString() {
    return getConfig().getTrimmed(KEY_REGISTRY_ZK_QUORUM,
        DEFAULT_ZK_HOSTS);
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
      ioe = new PathNotFoundException(path);
    } else if (exception instanceof KeeperException.NodeExistsException) {
      ioe = new FileAlreadyExistsException(path);
    } else if (exception instanceof KeeperException.NoAuthException) {
      ioe = new PathAccessDeniedException(path);
    } else if (exception instanceof KeeperException.NotEmptyException) {
      ioe = new PathIsNotEmptyDirectoryException(path);
    } else if (exception instanceof KeeperException.AuthFailedException) {
      ioe = new AuthenticationFailedException(path,
          "Authentication Failed: " + exception, exception);
    } else if (exception instanceof KeeperException.NoChildrenForEphemeralsException) {
      ioe = new NoChildrenForEphemeralsException(path, 
          "Cannot create a path under an ephemeral node: " + exception,
          exception);
    } else {
      ioe = new RegistryIOException(path,
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
  @VisibleForTesting
  public boolean maybeCreate(String path, CreateMode mode) throws IOException {
    List<ACL> acl = rootACL;
    return maybeCreate(path, mode, acl, false);
  }

  /**
   * Create a path if it does not exist. 
   * The check is poll + create; there's a risk that another process
   * may create the same path before the create() operation is executed/
   * propagated to the ZK node polled.
   *
   * @param path path to create
   * @param acl ACL for path -used when creating a new entry
   * @param createParents flag to trigger parent creation
   * @return true iff the path was created
   * @throws IOException
   */
  @VisibleForTesting
  public boolean maybeCreate(String path,
      CreateMode mode,
      List<ACL> acl,
      boolean createParents) throws IOException {
    return zkMkPath(path, mode, createParents, acl);
  }


  /**
   * Stat the file
   * @param path path of operation
   * @return a curator stat entry
   * @throws IOException on a failure
   * @throws PathNotFoundException if the path was not found
   */
  public Stat zkStat(String path) throws IOException {
    String fullpath = createFullPath(path);
    Stat stat;
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stat {}", fullpath);
      }
      stat = curator.checkExists().forPath(fullpath);
    } catch (Exception e) {
      throw operationFailure(fullpath, "read()", e);
    }
    if (stat == null) {
      throw new PathNotFoundException(path);
    }
    return stat;
  }
  
  public List<ACL> zkGetACLS(String path) throws IOException {
    String fullpath = createFullPath(path);
    List<ACL> acls;
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stat {}", fullpath);
      }
      acls = curator.getACL().forPath(fullpath);
    } catch (Exception e) {
      throw operationFailure(fullpath, "read()", e);
    }
    if (acls == null) {
      throw new PathNotFoundException(path);
    }
    return acls;
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
    } catch (PathNotFoundException e) {
      return false;
    } catch (Exception e) {
      throw operationFailure(path, "existence check", e);
    }
  }

  /**
   * Verify a path exists
   * @param path path of operation
   * @throws PathNotFoundException if the path is absent
   * @throws IOException
   */
  public String zkPathMustExist(String path) throws IOException {
    zkStat(path);
    return path;
  }

  /**
   * Create a path
   * @param path path to create
   * @param mode mode for path
   * @throws IOException
   */
  public void zkMkPath(String path, CreateMode mode) throws IOException {
    zkMkPath(path, mode, false, rootACL);
  }

  /**
   * Create a directory. It is not an error if it already exists
   * @param path path to create
   * @param mode mode for path
   * @param createParents flag to trigger parent creation
   * @param acl ACL for path 
   * @throws IOException any problem
   */
  public boolean zkMkPath(String path,
      CreateMode mode,
      boolean createParents,
      List<ACL> acl) 
      throws IOException {
    path = createFullPath(path);
    try {
      CreateBuilder createBuilder = curator.create();
      createBuilder.withMode(mode).withACL(acl);
      if (createParents) {
        createBuilder.creatingParentsIfNeeded();
      }
      createBuilder.forPath(path);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Created path {} with mode {} and ACL {}", path, mode, acl);
      }
    } catch (KeeperException.NodeExistsException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("path already present: {}", path, e);
      }
      return false;
    } catch (Exception e) {
      throw operationFailure(path, "mkdir() ", e);
    }
    return true;
  }

  /**
   * Recursively make a path
   * @param path path to create
   * @param acl ACL for path
   * @throws IOException any problem
   */
  public void zkMkParentPath(String path,
      List<ACL> acl) throws
      IOException {
    // split path into elements

      zkMkPath(RegistryPathUtils.parentOf(path), CreateMode.PERSISTENT, true, acl);
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
    Preconditions.checkArgument(data != null, "null data");
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
    Preconditions.checkArgument(data != null, "null data");
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
   * @param overwrite enable overwrite
   * @throws IOException
   * @return true if the entry was created, false if it was simply updated.
   */
  public boolean zkSet(String path,
      CreateMode mode,
      byte[] data,
      List<ACL> acl, boolean overwrite) throws IOException {
    Preconditions.checkArgument(data != null, "null data");
    if (!zkPathExists(path)) {
      zkCreate(path, mode, data, acl);
      return true;
    } else {
      if (overwrite) {
        zkUpdate(path, data);
        return false;
      } else {
        throw new FileAlreadyExistsException(path);
      }
    }
  }

  /**
   * Delete a directory/directory tree.
   * It is not an error to delete a path that does not exist
   * @param path path of operation
   * @param recursive flag to trigger recursive deletion
   * @param backgroundCallback callback; this being set converts the operation
   * into an async/background operation.
   * task
   * @throws IOException on problems other than no-such-path
   */
  public void zkDelete(String path,
      boolean recursive,
      BackgroundCallback backgroundCallback) throws IOException {
    String fullpath = createFullPath(path);
    try {
      LOG.debug("Deleting {}", fullpath);
      DeleteBuilder delete = curator.delete();
      if (recursive) {
        delete.deletingChildrenIfNeeded();
      }
      if (backgroundCallback != null) {
        delete.inBackground(backgroundCallback);
      }
      delete.forPath(fullpath);
    } catch (KeeperException.NoNodeException e) {
      // not an error
    } catch (Exception e) {
      throw operationFailure(fullpath, "delete()", e);
    }
  }

  /**
   * List all children of a path
   * @param path path of operation
   * @return a possibly empty list of children
   * @throws IOException
   */
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
   * Return a path dumper instance which can do a full dump
   * of the registry tree in its <code>toString()</code>
   * operation
   * @return a class to dump the registry
   */
  @VisibleForTesting
  public ZKPathDumper dumpPath() {
    return new ZKPathDumper(curator, registryRoot);
  }
}
