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

package org.apache.hadoop.yarn.registry.client.services;

import org.apache.zookeeper.ZooDefs;

/**
 * Internal constants for the registry.
 * 
 * These are the things which aren't visible to users.
 * 
 */
public interface RegistryInternalConstants {


  /**
   * Pattern of a hostname : {@value}
   */
  String HOSTNAME_PATTERN =
      "([a-z0-9]|[a-z0-9][a-z0-9\\-]*[a-z0-9])";
  /**
   * Header of a service record:  "jsonservicrec"
   * By making this over 12 bytes long, we can auto-determine which entries
   * in a listing are too short to contain a record without getting their data
   */
  byte[] RECORD_HEADER = {'j', 's', 'o', 'n', 
                          's', 'e','r','v','i', 'c', 'e',
                          'r','e','c'};
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
   * Name of the SASL auth provider which has to be added to ZK server to enable
   * sasl: auth patterns. Without this callers can connect via SASL, but
   * they can't use it in ACLs
   */
  String SASLAUTHENTICATION_PROVIDER =
      "org.apache.zookeeper.server.auth.SASLAuthenticationProvider";

  /**
   * String to use as the prefix when declaring a new auth provide.
   */
  String ZOOKEEPER_AUTH_PROVIDER = "zookeeper.authProvider";
}
