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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Interface that all encryption methods should support. 
 */
@Public
@Evolving
public interface Encryptor {
  /**
   * Initialize the service.
   * @param conf the configuration for the encryptor.
   */ 
  public void init(Configuration conf) throws Exception;

  /**
   * Start the encrptor
   */ 
  public void start() throws Exception;

  /**
   * Stop the encrptor
   */ 
  public void stop() throws Exception;

  /**
   * Encrypt the data to be stored.
   * @param data the data to encrypt
   * @return the encrypted data.
   * @throws RegistryException on any error.
   */ 
  public byte[] encrypt(byte[] data) throws RegistryException;

  /**
   * Decrypt the data.
   * @param data the data to decrypt
   * @return the decrypted data.
   * @throws RegistryException on any error.
   */ 
  public byte[] decrypt(byte[] data) throws RegistryException;
}

