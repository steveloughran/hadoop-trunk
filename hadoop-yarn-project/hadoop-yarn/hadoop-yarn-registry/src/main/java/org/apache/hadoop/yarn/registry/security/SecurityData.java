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
package org.apache.hadoop.yarn.registry.security;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;

import org.codehaus.jackson.JsonNode;

import java.util.concurrent.ConcurrentHashMap;
import java.util.ServiceLoader;

/**
 * Extension point for security data.  All methods must be thread safe.
 */
@Public
@Evolving
public abstract class SecurityData {
  private Configuration conf;

  /**
   * Initialize the class.
   * @param conf the configuration to use.
   */ 
  public void init(Configuration conf) {
    this.conf = conf;
  }

  /**
   * @return the Hadoop configuration.
   */ 
  protected Configuration getConf() {
    return this.conf;
  }

  /**
   * @return the name that this security data object will be keyed under.
   */ 
  public abstract String getName();

  /**
   * Validate the passed in parameters, generate any needed missing data, and
   * return an encoded byte array that will be stored. NOTE the returned bytes
   * will be encrypted before being stored.
   * @param securityParams the raw JSON parameters passed into the POST.
   * @return the encoded bytes for storage.
   */ 
  public abstract byte [] validateAndPrepareForStorage(JsonNode securityParams) throws SecurityDataException;

  /**
   * Get information for this security data that can be publicly viewed.
   * @param data the data returned by validateAndPrepareForStorage
   * @return data that can be shared publicly in a Json format. 
   */ 
  public abstract JsonNode getPublicInformation(byte [] data) throws SecurityDataException;

  /**
   * Get information for this security data that can be viewed by the owner.
   * @param data the data returned by validateAndPrepareForStorage
   * @return data that can be viewed by the owner in a Json format. 
   */ 
  public abstract JsonNode getAllInformation(byte [] data) throws SecurityDataException;

  private static ConcurrentHashMap<String, SecurityData> sdCache = null;
  private static ServiceLoader<SecurityData> loader = ServiceLoader.load(SecurityData.class);

  /**
   * Find the SecurityData implementation for the given name.
   */ 
  public static SecurityData getSDFor(String name) {
    return sdCache.get(name);
  }

  /**
   * Initialize and cache the SecurityData objects.
   * @param conf the configuration pass to each SecurityData object.
   */ 
  public synchronized static void initCache(Configuration conf) {
    sdCache = new ConcurrentHashMap<String, SecurityData>();
    for (SecurityData sd: loader) {
      sd.init(conf);
      sdCache.putIfAbsent(sd.getName(), sd);
    }
  }
}
