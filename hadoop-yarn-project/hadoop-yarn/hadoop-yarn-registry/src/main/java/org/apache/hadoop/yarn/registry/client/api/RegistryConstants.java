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

package org.apache.hadoop.yarn.registry.client.api;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.zookeeper.ZooDefs;

/**
 * Constants for the registry, including configuration keys and default
 * values.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface RegistryConstants {

  /**
   * prefix for registry configuration options: {@value}
   */
  String REGISTRY_PREFIX = "yarn.registry.";

  /**
   * flag to indicate whether or not the registry should
   * be enabled: {@value}
   */
  String KEY_REGISTRY_ENABLED = REGISTRY_PREFIX + "enabled";

  boolean DEFAULT_REGISTRY_ENABLED = false;


  /**
   * The default permissions for the registry root: {@value}
   */
  String WORLD_ANYONE_RWCDA = "world:anyone:rwcda";

  /**
   * System accounts for the registry: {@value}. 
   */
  String KEY_REGISTRY_SYSTEM_ACCESS = REGISTRY_PREFIX + "system.access";

  /**
   * trimmable comma separated list of system accounts: {@value}.
   * If there is an "@" at the end of an entry it 
   * instructs the registry to append the kerberos domain.
   */
  String DEFAULT_REGISTRY_SYSTEM_ACCESS = "hadoop,yarn,mapred";

  /**
   * The default ZK session timeout: {@value}
   */
  int DEFAULT_ZK_SESSION_TIMEOUT = 60000;
  /**
   * The default ZK connection timeout: {@value}
   */
  int DEFAULT_ZK_CONNECTION_TIMEOUT = 15000;
  /**
   * The default # of times to retry a ZK connection: {@value}
   */
  int DEFAULT_ZK_RETRY_TIMES = 5;
  /**
   * The default interval between connection retries: {@value}
   */
  int DEFAULT_ZK_RETRY_INTERVAL = 1000;
  /**
   * Default limit on retries: {@value}
   */
  int DEFAULT_ZK_RETRY_CEILING = 5;

  /**
   * Default root of the yarn registry: {@value}
   */
  String DEFAULT_REGISTRY_ROOT = "/yarnRegistry";
  
  /**
   * Pattern of a hostname : {@value}
   */
  String HOSTNAME_PATTERN =
      "([a-z0-9]|[a-z0-9][a-z0-9\\-]*[a-z0-9])";
  /**
   *  path to users off the root: {@value}
   */
  String PATH_USERS = "users/";
  /**
   *  path to system services off the root : {@value}
   */
  String PATH_SYSTEM_SERVICES = "services/";

  /**
   *  path under a service record to point to components of that service:
   *  {@value}
   */
  String SUBPATH_COMPONENTS = "/components";

  /**
   * Header of a service record: {@value}
   * By making this over 12 bytes long, we can auto-determine which entries
   * in a listing are too short to contain a record without getting their data
   */
  byte[] RECORD_HEADER = {'j', 's', 'o', 'n', 
                          's', 'e','r','v','i', 'c', 'e',
                          'r','e','c'};


  String ZK_PREFIX = REGISTRY_PREFIX + "zk.";
  /**
   * Flag to indicate whether the ZK service should be enabled {@value}
   * in the RM
   */
  String KEY_ZKSERVICE_ENABLED = ZK_PREFIX + "service.enabled";
  boolean DEFAULT_ZKSERVICE_ENABLED = false;


  /**
   * List of hostname:port pairs defining the ZK quorum: {@value}
   */
  String KEY_REGISTRY_ZK_QUORUM = ZK_PREFIX + "quorum";

  /**
   * Zookeeper session timeout in milliseconds: {@value}
   */
  String KEY_REGISTRY_ZK_SESSION_TIMEOUT =
      ZK_PREFIX + "session-timeout-ms";

  /**
   * Zookeeper connect retry count: {@value}
   */
  String KEY_REGISTRY_ZK_RETRY_TIMES = ZK_PREFIX + "retry.times";

  /**
   * Zookeeper connection timeout in milliseconds: {@value}
   */

  String KEY_REGISTRY_ZK_CONNECTION_TIMEOUT =
      ZK_PREFIX + "connection-timeout-ms";

  /**
   * Zookeeper connect interval in milliseconds: {@value}
   */
  String KEY_REGISTRY_ZK_RETRY_INTERVAL =
      ZK_PREFIX + "retry.interval-ms";

  /**
   * Zookeeper retry limit in milliseconds: {@value}
   */
  String KEY_REGISTRY_ZK_RETRY_CEILING =
      ZK_PREFIX + "retry.ceiling-ms";

  /**
   * for simple authentication, the auth password: {@value}.
   * If set it enables pass-based auth... this is exclusive
   * with Kerberos authentication.
   */
  String KEY_REGISTRY_AUTH_PASS = REGISTRY_PREFIX + "auth.pass";

  /**
   * For simple auth, the registry ID. This can override
   * anything worked out from the user (i.e. their shortname)
   */
  String KEY_REGISTRY_AUTH_ID = REGISTRY_PREFIX + "auth.id";

  /**
   * Key to set if the registry is secure. Turning it on
   * changes the permissions policy from "open access"
   * to restrictions on kerberos with the option of
   * a user adding one or more auth key pairs down their
   * own tree.
   */
  String KEY_REGISTRY_SECURE = REGISTRY_PREFIX + "secure";

  /**
   * Root path in the ZK tree for the registry: {@value}
   */
  String KEY_REGISTRY_ZK_ROOT = ZK_PREFIX + "root";


  /**
   * principal. If set, secure mode is expected
   */
  @Deprecated
  String KEY_REGISTRY_ZK_PRINCIPAL = ZK_PREFIX + "principal";


  /**
   * Key to define the JAAS context. IF set, it forces the
   * service into secure mode —which will require JAAS to have
   * been set up
   */
  String KEY_ZKSERVICE_JAAS_CONTEXT = ZK_PREFIX + "jaas.context";


  /**
   * ACL: {@value} for the registry root
   */
  String KEY_REGISTRY_ZK_ACL = ZK_PREFIX + "acl";

  /**
   * The default ZK quorum binding: {@value}
   */
  String DEFAULT_ZK_HOSTS = "localhost:2181";

  /**
   * ZK servertick time: {@value}
   */
  String KEY_ZKSERVICE_TICK_TIME = ZK_PREFIX + "ticktime";
  
  /**
   * host to register on
   */
  String KEY_ZKSERVICE_HOST = ZK_PREFIX + "host";


  /**
   * Default host to serve on -this is "localhost" as it
   * is the only one guaranteed to be available.
   */
  String DEFAULT_ZKSERVICE_HOST = "localhost";

  /**
   * port; 0 or below means "any": {@value}
   */
  String KEY_ZKSERVICE_PORT = ZK_PREFIX + "port";
  
  /**
   * Directory containing data: {@value}
   */
  String KEY_ZKSERVICE_DIR = ZK_PREFIX + ".dir";


  /**
   * Permissions for readers: {@value}.
   */
  int PERMISSIONS_REGISTRY_READERS = ZooDefs.Perms.READ;
  /**
   * Permissions for system services: {@value}
   */

  int PERMISSIONS_REGISTRY_SYSTEM_SERVICES =
      ZooDefs.Perms.ALL;
  /**
   * Permissions for a user's root entry: {@value}.
   * All except the admin permissions (ACL access) on a node
   */
  int PERMISSIONS_REGISTRY_USER_ROOT =
      ZooDefs.Perms.READ | ZooDefs.Perms.WRITE | ZooDefs.Perms.CREATE |
      ZooDefs.Perms.DELETE;
  /**
   * Permissions for any other user entry. Full access
   */
  int PERMISSIONS_REGISTRY_USER = ZooDefs.Perms.ALL;

}
