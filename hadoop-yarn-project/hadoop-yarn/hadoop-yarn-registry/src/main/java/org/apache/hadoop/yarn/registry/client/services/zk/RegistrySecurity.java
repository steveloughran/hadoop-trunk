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
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.Environment;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.ZooKeeperSaslServer;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.hadoop.yarn.registry.client.api.RegistryConstants.*;

/**
 * Implement the registry security ... standalone for easier testing
 */
public class RegistrySecurity {
  private static final Logger LOG =
      LoggerFactory.getLogger(RegistrySecurity.class);
  public static final String PERMISSIONS_REGISTRY_ROOT = "world:anyone:rwcda";
  public static final String PERMISSIONS_REGISTRY_SYSTEM = "world:anyone:rwcda";
  public static final String PERMISSIONS_REGISTRY_USERS = "world:anyone:rwcda";
  private final Configuration conf;
  private String domain;

  /**
   * Special ACL: world readable, but only the owner can write.
   * Implemented as copy-on-write, so can be extended without
   * impact on other uses.
   */
  public static List<ACL> WorldReadOwnerWriteACL;
  static {
    List<ACL> acls =  new ArrayList<ACL>();
    acls.add(new ACL(PERMISSIONS_REGISTRY_USER, ZooDefs.Ids.AUTH_IDS));
    acls.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));

    WorldReadOwnerWriteACL = new CopyOnWriteArrayList<ACL>(acls);
  }

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
   * Parse the IDs, adding a realm if needed, setting the permissions
   * @param idString id string
   * @param realm realm to add
   * @param perms permissions
   * @return the relevant ACLs
   */
  public List<ACL> parseIds(String idString, String realm, int perms) {
    List<String> aclPairs = splitAclPairs(idString);
    List<ACL> ids = new ArrayList<ACL>(aclPairs.size());
    for (String aclPair : aclPairs) {
      ACL newAcl = new ACL();
      newAcl.setId(parse(aclPair, realm));
      newAcl.setPerms(perms);
      ids.add(newAcl);
    }
    return ids;
  }


  /*
  Server {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  keyTab="/etc/zookeeper/conf/zookeeper.keytab"
  storeKey=true
  useTicketCache=false
  principal="zookeeper/fully.qualified.domain.name@<YOUR-REALM>";
};
   */
  /**
   * Printf string for the JAAS entry
   */

  /**
   * JAAS template: {@value}
   * Note the semicolon on the last entry
   */
  private static final String JAAS_ENTRY =
      "%s { \n"
      + " com.sun.security.auth.module.Krb5LoginModule required\n"
      // kerberos module
      + " keyTab=\"%s\"\n"
      + " principal=\"%s\"\n"
      + " useKeyTab=true\n"
      + " useTicketCache=false\n"
      + " storeKey=true;\n"
      + "}; \n";


  public String createJAASEntry(
      String role,
      String principal, File keytab) {
    return String.format(
        Locale.ENGLISH,
        JAAS_ENTRY,
        role,
        keytab.getAbsolutePath(),
        principal);
  }

  /**
   * Create and save a JAAS config file
   * @param dest destination
   * @param principal kerberos principal
   * @param keytab  keytab
   * @throws IOException trouble
   */
  public void buildJAASFile(File dest, String principal, File keytab) throws
      IOException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(principal),
        "invalid principal");

    if (!keytab.isFile()) {
      throw new FileNotFoundException("Keytab not found: " + keytab);
    }
    dest.delete();
    StringBuilder jaasBinding = new StringBuilder(256);
    jaasBinding.append(createJAASEntry("Server", principal, keytab));
    jaasBinding.append(createJAASEntry("Client", principal, keytab));
    FileUtils.write(dest, jaasBinding.toString());
  }


  /**
   * Prepare the JVM for JAAS auth. IF the system properties
   * do not already defined a JAAS file, it will be created
   * @param principal principal to auth as
   * @param keytabFile keytab file
   * @param jaasFileToCreate jaas file to create if required. Set this
   * to null to raise an IOException instead
   * @return the Jaas File
   * @throws IOException on any IO problem or a missing config
   */
  public File prepareJAASAuth(
      String principal, File keytabFile,
      File jaasFileToCreate) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Preparing JAAS auth for {} and keytab {}", principal,
          keytabFile);
    }
    if (!keytabFile.exists()) {
      throw new FileNotFoundException("Missing keytab "
                                      + keytabFile.getAbsolutePath());
    }
    System.setProperty(ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY,
        ZooKeeperSaslServer.DEFAULT_LOGIN_CONTEXT_NAME);
    String jaasFilename = System.getProperty(Environment.JAAS_CONF_KEY);
    File jaasFile;

    if (StringUtils.isEmpty(jaasFilename)) {
      if (jaasFileToCreate == null) {
        throw new IOException("No JAAS file specified in the system property"
                              + Environment.JAAS_CONF_KEY);
      }
      // set up jaas.
      jaasFile = jaasFileToCreate;
      buildJAASFile(jaasFile, principal, keytabFile);
      // 
    } else {
      jaasFile = new File(jaasFilename);
    }
    if (!jaasFile.isFile()) {
      throw new FileNotFoundException(
          " File specified in " + Environment.JAAS_CONF_KEY
          + " not found: " + jaasFile.getAbsolutePath());
    }
    // here the JAAS file is set up
    System.setProperty(Environment.JAAS_CONF_KEY, jaasFile.getAbsolutePath());
    return jaasFile;
  }

  /**
   * Reset any system properties related to JAAS
   */
  public static void clearJaasSystemProperties() {
    System.clearProperty(Environment.JAAS_CONF_KEY);

  }

  /**
   * Set the client properties. This forces the ZK client into 
   * failing if it can't auth
   * @param context
   */
  public static void setZKSaslClientProperties(String context) {
    System.setProperty(ZooKeeperSaslClient.ENABLE_CLIENT_SASL_KEY, "true");
    System.setProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY,
        context != null ? context : "Client");
  }

  /**
   * Clear all the ZK Sasl properties
   */
  public static void clearZKSaslProperties() {
    System.clearProperty(ZooKeeperSaslClient.ENABLE_CLIENT_SASL_KEY);
    System.clearProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY);
  }
  
  public String getKeytabConfOption() {
    return conf.getTrimmed(KEY_REGISTRY_ZK_KEYTAB);
  }


  public String getPrincipalConfOption() {
    return conf.getTrimmed(KEY_REGISTRY_ZK_PRINCIPAL);
  }

  public boolean isSecurityEnabled() {
    return StringUtils.isNotEmpty(getPrincipalConfOption());
  }


  /**
   * Get the keytab file referred to in a configuration
   * @return the keytab or  null
   */
  public File getKeytabConfFile() throws FileNotFoundException {
    String zkKeytab = getKeytabConfOption();
    if (StringUtils.isNotEmpty(zkKeytab)) {
      return new File(zkKeytab);
    } else {
      throw new FileNotFoundException("No keytab file specified");
    }
  }
  
  public void logCurrentUser() {
    try {
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      LOG.info("Current user = {}",currentUser);
      UserGroupInformation realUser = currentUser.getRealUser();
      LOG.info("Real User = {}" , realUser);
    } catch (IOException e) {
      LOG.warn("Failed to get current user {}, {}", e);
    }
  }

  public void logACLs(List<ACL> acls) {
    for (ACL acl : acls) {
      LOG.info("{}", acl.toString());
    }
  }
}
