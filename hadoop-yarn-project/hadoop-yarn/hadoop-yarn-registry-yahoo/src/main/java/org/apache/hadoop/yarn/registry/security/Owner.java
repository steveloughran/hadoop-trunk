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

import org.apache.hadoop.yarn.registry.RegistryException;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;

import javax.servlet.http.HttpServletRequest;

import java.util.concurrent.ConcurrentHashMap;
import java.util.ServiceLoader;
import java.util.Collection;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Extension point for validating users.  All methods must be thread safe.
 */
@Public
@Evolving
public abstract class Owner {
  private static final Log LOG = LogFactory.getLog(Owner.class);
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
   * @return the scheme that this owner supports
   */ 
  public abstract String getScheme();

  /**
   * Check to see if the given user is in the given request.
   * @param req the servlet request to check
   * @param id the id of the user to check against
   * @return true if they match else false
   */ 
  public abstract boolean matches(HttpServletRequest req, String id) throws RegistryException;

  public static final String REGISTRY_ADMINS = "yarn.registry.admins";

  private static ConcurrentHashMap<String, Owner> ownerCache = null;
  private static Collection<String> admins = null;
  private static ServiceLoader<Owner> loader = ServiceLoader.load(Owner.class);

  public static void assertIsAdmin(HttpServletRequest req) throws RegistryException {
    if (!isInList(req, admins)) {
      throw new RegistryException("The current user is not allowed to perform this operation", 403);
    }
  }

  public static boolean isOwnerOrAdmin(HttpServletRequest req, Collection<String> owners) throws RegistryException {
    return (isInList(req, admins) || isInList(req, owners));
  }

  public static void assertIsOwnerOrAdmin(HttpServletRequest req, Collection<String> owners) throws RegistryException {
    if (!isOwnerOrAdmin(req, owners)) {
      throw new RegistryException("The current user is not allowed to perform this operation", 403);
    }
  }

  private static boolean isInList(HttpServletRequest req, Collection<String> toCheck) throws RegistryException {
    if (toCheck == null) {
      return false;
    }
    for (String id: toCheck) {
      String [] parts = id.split(":", 2);
      String scheme;
      String user;
      if (parts.length == 1) {
        scheme = "user";
        user = parts[0];
      } else {
        scheme = parts[0];
        user = parts[1];
      }
      Owner owner = ownerCache.get(scheme);
      if (owner == null) {
        LOG.warn("No Owner found that supports the scheme "+scheme);
        continue;
      }
      if (owner.matches(req, user)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Initialize and cache the Owner objects.
   * @param conf the configuration pass to each Owner object.
   */ 
  public synchronized static void initCache(Configuration conf) {
    String [] tmpAdmins = conf.getStrings(REGISTRY_ADMINS);
    if (tmpAdmins != null) {
      admins = Arrays.asList(tmpAdmins);
    }
    ownerCache = new ConcurrentHashMap<String, Owner>();
    for (Owner owner: loader) {
      owner.init(conf);
      ownerCache.putIfAbsent(owner.getScheme(), owner);
    }
  }
}
