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

import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Interface that all storage back ends for the registry srvice must implement.
 */
@Public
@Evolving
public abstract class Storage {
  public static interface UpdateFunc<T> {
    /**
     * Update input and return it. Should have no side effects at all.
     * @param intput the item to update
     * @return the updated item
     * @throws StorageException on any error.
     */   
    public T update(T input) throws StorageException;
  }

  private Configuration conf;

  /**
   * Initialize the service.
   * @param conf the configuration for the server.
   */ 
  public void init(Configuration conf) throws Exception {
    this.conf = conf;
  }

  /**
   * @return the configuration.
   */   
  public Configuration getConf() {
    return conf;
  }

  /**
   * Start the service running
   */ 
  public void start() throws Exception {
    //Empty
  }

  /**
   * Storp the service
   */ 
  public void stop() throws Exception {
    //Empty
  }

  private static final Pattern validHostPattern = Pattern.compile("^[a-z0-9.-]+$", Pattern.CASE_INSENSITIVE);

  /**
   * validates that the virtual host key is valid, must be called before 
   * storing a virtual host.  This means it is not null and matches what a
   * virtual host should look like.  It can only have in it a-z, 0-9, '.' and '-'. 
   * @param key the key to check
   * @throws StorageException if it does not match.
   */ 
  public static void validateVHKey(String key) throws StorageException {
    if (key == null) {
      throw new StorageException("the virtual host name cannot be null", 400);
    }
    if (!validHostPattern.matcher(key).matches()) {
      throw new StorageException(key+" does not appear to be a valid host name, only a-z, 0-9, '.' and '-' are allowed.", 400);
    }
  }

  /**
   * validate that the value is not null.
   * @param value the value to validate.
   */ 
  public static void validateValue(byte[] value) throws StorageException {
    if (value == null) {
      throw new StorageException("The value stored cannot be null", 500);
    }
  }

  /**
   * validate that the server is a valid host name too.
   * @param key the server name to validate.
   */ 
  public static void validateServerKey(String key) throws StorageException {
    if (key == null) {
      throw new StorageException("the server name cannot be null", 500);
    }
    if (!validHostPattern.matcher(key).matches()) {
      throw new StorageException(key+" does not appear to be a valid server name, only a-z, 0-9, '.' and '-' are allowed.", 400);
    }
  }

  /**
   * Add a new virtual host entry.
   * @param key the key to store the virtual host under.
   * @param value the bytes to store for the virtual host.
   * @throws DuplicateEntryException if the key is already used.
   * @throws StorageException if the key is not valid any other internal error.
   */ 
  public abstract void addVirtualHost(String key, byte[] value) throws StorageException;

  /**
   * Atomically update a virtual host entry.
   * @param key the virtual host to update.
   * @param updated the function that will be used to update the virtual host entry.
   * The storage implementation could implement atomic updates in many different ways.
   * The function should be written so it has no side effects because it could be
   * called multiple times if the first time it was called the update did not complete
   * atomically.
   * @throws StorageException on any error.
   */ 
  public abstract void updateVirtualHost(String key, UpdateFunc<byte[]> updater) throws StorageException;

  /**
   * @return a listing of all of the virtual host keys.
   * @throws StorageException on any error.
   */ 
  public abstract Collection<String> listVirtualHostKeys() throws StorageException;

  /**
   * Get a specific virtual host entry.
   * @param key the key to find the virtual host under.
   * @return the bytes for the virtual host.
   * @throws EntryNotFoundException if the entry does not exist.
   * @throws StorageException on any other error.
   */ 
  public abstract byte[] getVirtualHost(String key) throws StorageException;

  /**
   * Delete a virtual host entry. 
   * @param key the key to find the virtual host under.
   * @throws EntryNotFoundException if the entry does not exist.
   * @throws StorageException on any other error.
   */ 
  public abstract void deleteVirtualHost(String key) throws StorageException;

  /**
   * Store a server entry.
   * @param vh the virtual host to store this under.
   * @param key the key the server is stored under.
   * @param value the bytes to store for the server.
   * @throws StorageException on any error.
   */ 
  public abstract void putServer(String vh, String key, byte[] value) throws StorageException;

  /**
   * Get a server entry.
   * @param vh the virtual host to store this under.
   * @param key the key to find the server under.
   * @return the bytes for the server.
   * @throws EntryNotFoundException if the entry does not exist.
   * @throws StorageException on any other error.
   */ 
  public abstract byte[] getServer(String vh, String key) throws StorageException;

  /**
   * Get all the Servers that are between fromKey and toKey
   * @param vh the virtual host to look for the keys under.
   * @return the servers associated with this virtual host
   * @throws StorageException on any error.
   */ 
  public abstract Collection<byte[]> listServers(String vh) throws StorageException;

  /**
   * Delete all the Servers that are for a given virtual host
   * @param vh the virtual host to delete servers under.
   * @throws StorageException on any error.
   */ 
  public abstract void deleteServers(String vh) throws StorageException;

  /**
   * Delete a server entry. 
   * @param vh the virtual host this server is under.
   * @param key the key to find the server under.
   * @throws EntryNotFoundException if the entry does not exist.
   * @throws StorageException on any other error.
   */ 
  public abstract void deleteServer(String vh, String key) throws StorageException;
}

