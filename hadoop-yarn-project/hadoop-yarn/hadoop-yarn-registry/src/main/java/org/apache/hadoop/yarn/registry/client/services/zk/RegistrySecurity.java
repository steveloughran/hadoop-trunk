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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implement the registry security ... standalone for easier testing
 */
public class RegistrySecurity {

  public static final String PERMISSIONS_REGISTRY_ROOT = "world:anyone:rwcda";
  public static final String PERMISSIONS_REGISTRY_SYSTEM = "world:anyone:rwcda";
  public static final String PERMISSIONS_REGISTRY_USERS = "world:anyone:rwcda";
  private final Configuration conf;
  private String domain;


  /**
   * Create an instance
   */
  public RegistrySecurity(Configuration conf) {
    this.conf = conf;
    try {
      MessageDigest.getInstance("SHA1");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e.toString(), e);
    }
  }
/*
  public String extractCurrentDomain() throws IOException {
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    UserGroupInformation realUser = currentUser.getRealUser();
    realUser.g
  }
  */
  
  /**
   * Generate a base-64 encoded digest of the password
   * @param password pass
   * @return a string that can be used for authentication
   */
  public String digest(String password) throws NoSuchAlgorithmException {
    try {
      return DigestAuthenticationProvider.generateDigest(password);
    } catch (NoSuchAlgorithmException e) {
      // because this gets caught in the constructor, this will never happen.
      throw new RuntimeException(e.toString(), e);

    }
  }

  public List<String> splitAclPairs(String aclString) {
    return Lists.newArrayList(
        Splitter.on(',').omitEmptyStrings().trimResults()
                .split(aclString));
  }

  /**
   * Parse a string down to an ID, adding a domain if needed
   * @param a id source
   * @param domain domain to add
   * @return the ID.
   */
  public Id parse(String a, String domain) {
    int firstColon = a.indexOf(':');
    int lastColon = a.lastIndexOf(':');
    if (firstColon == -1 || lastColon == -1 || firstColon != lastColon) {
      throw new ZKUtil.BadAclFormatException(
          "ACL '" + a + "' not of expected form scheme:id");
    }
    String scheme = a.substring(0, firstColon);
    String id = a.substring(firstColon + 1);
    if (id.endsWith("@")) {
      Preconditions.checkArgument(
          StringUtils.isNotEmpty(domain),
          "@ suffixed account but no domain %s", id);
      id = id + domain;
    }
    return new Id(scheme, id);

  }


  /**
   * Parse the IDs, adding a domain if needed, setting the permissions
   * @param idString id string
   * @param domain domain to add
   * @param perms permissions
   * @return the relevant ACLs
   */
  public List<ACL> parseIds(String idString, String domain, int perms) {
    List<String> aclPairs = splitAclPairs(idString);
    List<ACL> ids = new ArrayList<ACL>(aclPairs.size());
    for (String aclPair : aclPairs) {
      ACL newAcl = new ACL();
      newAcl.setId(parse(aclPair, domain));
      newAcl.setPerms(perms);
      ids.add(newAcl);
    }
    return ids;
  }




}
