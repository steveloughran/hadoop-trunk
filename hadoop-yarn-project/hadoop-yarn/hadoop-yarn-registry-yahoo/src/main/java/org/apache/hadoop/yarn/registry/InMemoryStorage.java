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

import org.apache.hadoop.yarn.registry.Storage.UpdateFunc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Collection;
import java.util.TreeMap;

/**
 * In memory Storage implementation.  Used for testing.
 */
public class InMemoryStorage extends Storage {
  private HashMap<String, byte[]> hosts = new HashMap<String, byte[]>();
  private TreeMap<String, byte[]> servers = new TreeMap<String, byte[]>();

  @Override 
  public synchronized void addVirtualHost(String key, byte[] value) throws StorageException {
    validateVHKey(key);
    validateValue(value);
    if (hosts.containsKey(key)) {
      throw new DuplicateEntryException(key+" is already defined for virtual hosts");
    }
    hosts.put(key, Arrays.copyOf(value, value.length));
  }

  @Override
  public synchronized void updateVirtualHost(String key, UpdateFunc<byte[]> updater) throws StorageException {
    byte [] ret = updater.update(getVirtualHost(key));
    validateValue(ret);
    hosts.put(key, ret);
  }

  @Override
  public synchronized Collection<String> listVirtualHostKeys() {
    return hosts.keySet();
  }

  @Override 
  public synchronized byte[] getVirtualHost(String key) throws StorageException {
    validateVHKey(key);
    byte [] ret = hosts.get(key);
    if (ret == null) {
      throw new EntryNotFoundException(key + " is not a registered virtual host");
    }
    return ret;
  }

  @Override 
  public synchronized void deleteVirtualHost(String key) throws StorageException {
    validateVHKey(key);
    if (hosts.remove(key) == null) {
      throw new EntryNotFoundException(key + " is not a registered virtual host");
    }
  }

  @Override 
  public synchronized void putServer(String vh, String key, byte[] value) throws StorageException {
    validateVHKey(vh);
    validateServerKey(key);
    validateValue(value);
    servers.put(vh+":"+key, Arrays.copyOf(value, value.length));
  }

  @Override 
  public synchronized byte[] getServer(String vh, String key) throws StorageException {
    validateVHKey(vh);
    validateServerKey(key);
    byte [] ret = servers.get(vh+":"+key);
    if (ret == null) {
      throw new EntryNotFoundException("no server found under "+vh+":"+key);
    }
    return ret;
  }

  @Override 
  public synchronized Collection<byte[]> listServers(String vh) throws StorageException {
    validateVHKey(vh);
    return servers.subMap(vh+":", vh+";").values();
  }

  @Override 
  public synchronized void deleteServers(String vh) throws StorageException {
    validateVHKey(vh);
    servers.subMap(vh+":", vh+";").clear();
  }

  @Override 
  public synchronized void deleteServer(String vh, String key) throws StorageException {
    validateVHKey(vh);
    validateServerKey(key);
    if (servers.remove(vh+":"+key) == null) {
      throw new EntryNotFoundException("no server found under "+key);
    }
  }
}

