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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.io.File;
import java.net.ServerSocket;
import java.net.InetSocketAddress;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.registry.Storage.UpdateFunc;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ServerCnxnFactory;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;

public class TestZkStorage {

  private Storage storage;

  private ZooKeeperServer server;
  private ServerCnxnFactory factory;
  private int port;
  private File tempDir;


  @Before
  public void setup() throws Exception {
    if (server != null || tempDir != null || storage != null) {
      cleanup();
    }
    ServerSocket s = new ServerSocket(0);
    port = s.getLocalPort();
    s.close();
    tempDir = File.createTempFile("zookeeper","data");
    tempDir.delete();
    tempDir.mkdirs();
    server = new ZooKeeperServer(tempDir, tempDir, 20000);
    factory = ServerCnxnFactory.createFactory();
    factory.configure(new InetSocketAddress("0.0.0.0",port), 10);
    factory.startup(server);

    Configuration conf = new Configuration(false);
    conf.set(ZkStorage.ZK_HOSTS, "localhost:"+port);
    storage = new ZkStorage();
    storage.init(conf);
    storage.start();
  }

  @After
  public void cleanup() throws Exception {
    if (storage != null) {
      storage.stop();
      storage = null;
    }
    if (server != null) {
      if (server.isRunning()) {
        server.shutdown();
      }
      server = null;
      factory = null;
    }
    if (tempDir != null) {
      FileUtil.fullyDelete(tempDir);
      tempDir = null;
    }
  }

  public static <T> void assertCollectionsEquiv(Collection<T> expected, Collection<T> found) {
    assertTrue("extra things found in result \nexpected: "+expected+"\n    found: "+found, expected.containsAll(found));
    assertTrue("missing things found in result \nexpected: "+expected+"\n    found: "+found, found.containsAll(expected));
  }

  @Test
  public void testAddGetDeleteVirtualHost() throws StorageException {
    byte[] vhostData = new byte[] {0,0,0,1};
    storage.addVirtualHost("test", vhostData);
    byte[] found = storage.getVirtualHost("test");
    assertArrayEquals(vhostData, found);
    assertCollectionsEquiv(Arrays.asList("test"), storage.listVirtualHostKeys());
    final byte[] updatedVhostData = new byte[] {0,0,0,2};
    storage.updateVirtualHost("test", new UpdateFunc<byte[]>() {
      @Override public byte[] update(byte [] data) {
        return updatedVhostData;
      }
    });
    found = storage.getVirtualHost("test");
    assertArrayEquals(updatedVhostData, found);
 
    storage.deleteVirtualHost("test");
    try {
      storage.getVirtualHost("test");
      fail("found a deleted virtual host");
    } catch (EntryNotFoundException e) {
      //Empty
    }
    assertCollectionsEquiv(new ArrayList<String>(), storage.listVirtualHostKeys());
  }

  @Test(expected=DuplicateEntryException.class)
  public void testAddDuplicateVirtualHost() throws StorageException {
    storage.addVirtualHost("test", new byte[]{0,1});
    storage.addVirtualHost("test", new byte[]{0,2});
  }

  @Test(expected=StorageException.class)
  public void testAddBadVirtualHostKey() throws StorageException {
    storage.addVirtualHost(null, new byte[]{0,1});
  }

  @Test(expected=StorageException.class)
  public void testAddBadVirtualHost() throws StorageException {
    storage.addVirtualHost("test", null);
  }

  @Test(expected=StorageException.class)
  public void testGetBadVirtualHost() throws StorageException {
    storage.getVirtualHost(null);
  }

  @Test(expected=EntryNotFoundException.class)
  public void testGetMissingVirtualHost() throws StorageException {
    storage.getVirtualHost("test");
  }

  @Test(expected=StorageException.class)
  public void testDeleteBadVirtualHost() throws StorageException {
    storage.deleteVirtualHost(null);
  }

  @Test(expected=EntryNotFoundException.class)
  public void testDeleteMissingVirtualHost() throws StorageException {
    storage.deleteVirtualHost("test");
  }

  @Test
  public void testAddGetDeleteServer() throws StorageException {
    storage.addVirtualHost("test", new byte[]{0,1});
    byte [] expected = new byte[]{0,2};
    storage.putServer("test","server1", expected);
    byte [] found = storage.getServer("test","server1");
    assertArrayEquals(expected, found);
    storage.deleteServer("test","server1");
    try {
      storage.getServer("test","server1");
      fail("found a deleted server");
    } catch (EntryNotFoundException e) {
      //Empty
    }
    storage.putServer("test","server1", expected);
    Collection<byte[]> s = storage.listServers("test");
    assertEquals(1, s.size());
    assertArrayEquals(expected, s.iterator().next());

    //Put in the same one again
    storage.putServer("test","server1", expected);
    s = storage.listServers("test");
    assertEquals(1, s.size());

    byte [] expected2 = new byte[]{1,0};
    storage.putServer("test","server2", expected2);
    s = storage.listServers("test");
    assertEquals(2, s.size());
    
    storage.deleteServers("test");
    storage.deleteVirtualHost("test");
    try {
      storage.getServer("test","server1");
      fail("found a deleted server");
    } catch (EntryNotFoundException e) {
      //Empty
    }
  }

  @Test(expected=StorageException.class)
  public void testPutServerBadKey() throws StorageException {
    storage.putServer("test", null, new byte[]{0,1});
  }

  @Test(expected=StorageException.class)
  public void testPutServerBadVHKey() throws StorageException {
    storage.putServer(null, "test", new byte[]{0,1});
  }

  @Test(expected=StorageException.class)
  public void testPutServerBadValue() throws StorageException {
    storage.putServer("test","1", null);
  }

  @Test(expected=StorageException.class)
  public void testGetServerBadKey() throws StorageException {
    storage.getServer("test",null);
  }

  @Test(expected=StorageException.class)
  public void testGetServerBadVHKey() throws StorageException {
    storage.getServer("$$$$","test");
  }

  @Test(expected=StorageException.class)
  public void testListServersKey() throws StorageException {
    storage.listServers(null);
  }

  @Test(expected=StorageException.class)
  public void testDeleteServerBadKey() throws StorageException {
    storage.deleteServer("test", null);
  }

  @Test(expected=StorageException.class)
  public void testDeleteServerBadVHKey() throws StorageException {
    storage.deleteServer(null, "test");
  }

  @Test(expected=StorageException.class)
  public void testDeleteServersKey() throws StorageException {
    storage.deleteServers(null);
  }
}

