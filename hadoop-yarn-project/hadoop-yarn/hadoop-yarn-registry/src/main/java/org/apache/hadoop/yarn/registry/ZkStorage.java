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

import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ZKUtil;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.ACL;
import org.apache.curator.framework.CuratorFrameworkFactory; 
import org.apache.curator.framework.CuratorFramework; 
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.RetrySleeper;
import org.apache.curator.RetryLoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Storage backed by ZooKeeper
 */
public class ZkStorage extends Storage {
  public static final Log LOG = LogFactory.getLog(ZkStorage.class);

  public static final String ZK_HOSTS = "yarn.registry.zk.connect-hosts";
  public static final String ZK_SESSION_TIMEOUT = "yarn.registry.zk.session-timeout-ms";
  public static final String ZK_CONNECTION_TIMEOUT = "yarn.registry.zk.connection-timeout-ms";
  public static final String ZK_ROOT = "yarn.registry.zk.root-node";
  public static final String ZK_ACL = "yarn.registry.zk.acl";
  public static final String ZK_RETRY_TIMES = "yarn.registry.zk.retry.times";
  public static final String ZK_RETRY_INTERVAL = "yarn.registry.zk.retry.interval-ms";
  public static final String ZK_RETRY_CEILING = "yarn.registry.zk.retry.ceiling-ms";

  private static final String BASE_VH_PATH = "/vh";

  private static final RetrySleeper sleeper = new RetrySleeper()
  {
    @Override
    public void sleepFor(long time, TimeUnit unit) throws InterruptedException {
        unit.sleep(time);
    }
  };

  private CuratorFramework zk;
  private List<ACL> zkAcl;
  private BoundedExponentialBackoffRetry retry;


  @Override
  public void start() throws Exception {
    Configuration conf = getConf();

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
    if (!exists("/vh")) {
      mkdir("/vh");
    }
  }

  @Override
  public void stop() {
    if (zk != null) {
      zk.close();
    }
  }

  private CuratorFramework newCurator(String root) {
    Configuration conf = getConf();
    String connectString = conf.get(ZK_HOSTS, "localhost:2181") + root;
    int sessionTimeout = conf.getInt(ZK_SESSION_TIMEOUT, 20000);
    int connectionTimeout = conf.getInt(ZK_CONNECTION_TIMEOUT, 15000);
    int retryTimes = conf.getInt(ZK_RETRY_TIMES, 5);
    int retryInterval = conf.getInt(ZK_RETRY_INTERVAL, 1000);
    int retryCeiling = conf.getInt(ZK_RETRY_CEILING, 30000);

    CuratorFrameworkFactory.Builder b = CuratorFrameworkFactory.builder();
    b.connectString(connectString)
     .connectionTimeoutMs(connectionTimeout)
     .sessionTimeoutMs(sessionTimeout)
     .retryPolicy(new BoundedExponentialBackoffRetry(retryInterval, retryTimes, retryCeiling));
    
    CuratorFramework ret = b.build();
    ret.start();
 
    return ret; 
  }

  private boolean exists(String path) throws StorageException {
    try {
      return zk.checkExists().forPath(path) != null;
    } catch (Exception e) {
      throw new StorageException("error checking for existance of "+path, e);
    }
  }

  private void mkdir(String path) throws StorageException {
    try {
      zk.create().withACL(zkAcl).forPath(path);
    } catch (Exception e) {
      throw new StorageException("error trying to mkdirs "+path, e);
    }
  }

  private void create(String path, byte[] data) throws StorageException {
    try {
      zk.create().withACL(zkAcl).forPath(path, data);
    } catch (Exception e) {
      throw new StorageException("error trying to create "+path, e);
    }
  }

  private void update(String path, byte[] data) throws StorageException {
    try {
      zk.setData().forPath(path, data);
    } catch (Exception e) {
      throw new StorageException("error updateing "+path, e);
    }
  }

  private void set(String path, byte[] data) throws StorageException {
    if (!exists(path)) {
      create(path, data);
    } else {
      update(path, data);
    }
  }

  private byte[] read(String path) throws StorageException {
    try {
      return zk.getData().forPath(path);
    } catch (KeeperException.NoNodeException e) {
      return null;
    } catch (Exception e) {
      throw new StorageException("error reading "+path, e);
    }
  }

  private void delete(String path) throws StorageException {
    try {
      zk.delete().guaranteed().forPath(path);
    } catch (Exception e) {
      throw new StorageException("error deleteing "+path, e);
    }
  }

  private List<String> list(String path) throws StorageException {
    try {
      return zk.getChildren().forPath(path);
    } catch (Exception e) {
      throw new StorageException("error listing "+path, e);
    }
  }

  private String path(String vh) {
    return BASE_VH_PATH + "/"+vh;
  }

  private String path(String vh, String server) {
    return path(vh)+"/"+server;
  }

  @Override 
  public void addVirtualHost(String key, byte[] value) throws StorageException {
    validateVHKey(key);
    validateValue(value);
    String path = path(key);
    if (exists(path)) {
      throw new DuplicateEntryException(key+" is already defined for virtual hosts");
    }
    create(path, value);
  }

  @Override
  public synchronized void updateVirtualHost(String key, UpdateFunc<byte[]> updater) throws StorageException {
    validateVHKey(key);
    String p = path(key);
    int retryCount = 0;
    long start = System.currentTimeMillis();
    while (true) {
      try {
        Stat stat = new Stat();
        byte [] data = zk.getData().storingStatIn(stat).forPath(p);
        byte [] ret = updater.update(data);
        validateValue(ret);
        zk.setData().withVersion(stat.getVersion()).forPath(p, ret);
        return;
      } catch (KeeperException.BadVersionException ve) {
        if (retry.allowRetry(retryCount, System.currentTimeMillis()-start, sleeper)) {
          retryCount++;
          LOG.info("tried to update "+p+" but version changed, retrying...");
        } else {
          throw new StorageException("Failed to update virtual host "+key+" after "+retryCount+" retries", ve);
        }
      } catch (KeeperException.NoNodeException e) {
        throw new EntryNotFoundException(key + " is not a registered virtual host");
      } catch (StorageException se) {
        throw se;
      } catch (Exception e) {
        throw new StorageException("Error trying to update virtual host "+key+" "+e.getMessage(), e);
      }
    }
  }

  @Override
  public synchronized Collection<String> listVirtualHostKeys() throws StorageException {
    return list(BASE_VH_PATH);
  }

  @Override 
  public byte[] getVirtualHost(String key) throws StorageException {
    validateVHKey(key);
    String path = path(key);
    byte [] ret = read(path);
    if (ret == null) {
      throw new EntryNotFoundException(key + " is not a registered virtual host");
    }
    return ret;
  }

  @Override 
  public void deleteVirtualHost(String key) throws StorageException {
    validateVHKey(key);
    String path = path(key);
    if (!exists(path)) {
      throw new EntryNotFoundException(key + " is not a registered virtual host");
    }
    deleteServers(key);
    delete(path);
  }

  @Override 
  public void putServer(String vh, String key, byte[] value) throws StorageException {
    validateVHKey(vh);
    validateServerKey(key);
    validateValue(value);

    String vhPath = path(vh);
    String path = path(vh, key);
    if (!exists(vhPath)) {
      throw new EntryNotFoundException(vh + " is not a registered virtual host");
    }
    set(path, value);
  }

  @Override 
  public byte[] getServer(String vh, String key) throws StorageException {
    validateVHKey(vh);
    validateServerKey(key);

    String vhPath = path(vh);
    String path = path(vh, key);
    if (!exists(vhPath)) {
      throw new EntryNotFoundException(vh + " is not a registered virtual host");
    }

    byte [] ret = read(path);
    if (ret == null) {
      throw new EntryNotFoundException("no server found under "+vh+" "+key);
    }
    return ret;
  }

  @Override 
  public Collection<byte[]> listServers(String vh) throws StorageException {
    validateVHKey(vh);

    String path = path(vh);
    if (!exists(path)) {
      throw new EntryNotFoundException(vh + " is not a registered virtual host");
    }
    List<String> keys = list(path);
    ArrayList<byte[]> ret = new ArrayList<byte[]>(keys.size());
    for (String key: keys) {
      String child = path(vh, key);
      byte [] found = read(child);
      if (found != null) {
        ret.add(found);
      }
    }

    return ret;
  }

  @Override 
  public void deleteServers(String vh) throws StorageException {
    validateVHKey(vh);

    String path = path(vh);
    if (!exists(path)) {
      throw new EntryNotFoundException(vh + " is not a registered virtual host");
    }
    for (String key: list(path)) {
      String child = path(vh, key);
      delete(child);
    }
  }

  @Override 
  public void deleteServer(String vh, String key) throws StorageException {
    validateVHKey(vh);
    validateServerKey(key);

    String vhPath = path(vh);
    String path = path(vh, key);
    if (!exists(vhPath)) {
      throw new EntryNotFoundException(vh + " is not a registered virtual host");
    }

    if (!exists(path)) {
      throw new EntryNotFoundException(key + " is not a registered server under "+vh);
    }

    delete(path);
  }
}

