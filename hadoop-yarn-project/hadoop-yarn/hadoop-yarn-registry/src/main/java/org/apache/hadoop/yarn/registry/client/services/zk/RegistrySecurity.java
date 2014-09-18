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
import org.apache.hadoop.security.authentication.util.KerberosUtil;
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

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.hadoop.yarn.registry.client.services.zk.ZookeeperConfigOptions.*;
import static org.apache.zookeeper.client.ZooKeeperSaslClient.*;

/**
 * Implement the registry security ... standalone for easier testing
 */
public class RegistrySecurity {
  private static final Logger LOG =
      LoggerFactory.getLogger(RegistrySecurity.class);
  public static final String PERMISSIONS_REGISTRY_ROOT = "world:anyone:rwcda";
  public static final String PERMISSIONS_REGISTRY_SYSTEM = "world:anyone:rwcda";
  public static final String PERMISSIONS_REGISTRY_USERS = "world:anyone:rwcda";
  public static final String CLIENT = "Client";
  public static final String SERVER = "Server";
  private static File lastSetJAASFile;
  private final Configuration conf;
  private final String idPassword;
  private String domain;

  public static final List<ACL> WorldReadWriteACL;

  static {
    List<ACL> acls = new ArrayList<ACL>();
    acls.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE));
    WorldReadWriteACL = new CopyOnWriteArrayList<ACL>(acls);

  }

  private List<ACL> system_principals;

  /**
   * Create an instance with no password
   */
  public RegistrySecurity(Configuration conf) throws IOException {
    this(conf, "");
  }

  /**
   * Create an instance
   * @param conf config
   * @param idPassword id:pass pair. If not empty, this tuple is validated
   * @throws IOException
   */
  public RegistrySecurity(Configuration conf, String idPassword) throws
      IOException {
    this.conf = conf;
    
    this.idPassword = idPassword;
    if (!StringUtils.isEmpty(idPassword)) {
      if (!isValid(idPassword)) {
        throw new IOException("Invalid id:password: " + idPassword);
      }
      digest(idPassword);
    }
  }

  /**
   * Check for an id:password tuple being valid. 
   * This test is stricter than that in {@link DigestAuthenticationProvider},
   * which splits the string, but doesn't check the contents of each
   * half for being non-"".
   * @param idPasswordPair id:pass pair
   * @return true if the pass is considered valid.
   */
  public boolean isValid(String idPasswordPair) {
    String parts[] = idPasswordPair.split(":");
    return parts.length == 2 
           && !StringUtils.isEmpty(parts[0])
           && !StringUtils.isEmpty(parts[1]);
  }
/*
  public String extractCurrentDomain() throws IOException {
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    UserGroupInformation realUser = currentUser.getRealUser();
    realUser.g
  }
  */

  /**
   * Generate a base-64 encoded digest of the idPasswordPair pair
   * @param idPasswordPair id:password
   * @return a string that can be used for authentication
   */
  public String digest(String idPasswordPair) throws
      IOException {
    if (StringUtils.isEmpty(idPasswordPair) || !isValid(idPasswordPair)) {
      throw new IOException("Invalid id:password: " + idPasswordPair);
    }
    try {
      return DigestAuthenticationProvider.generateDigest(idPasswordPair);
    } catch (NoSuchAlgorithmException e) {
      // unlikely since it is standard to the JVM, but maybe JCE restrictions
      // could trigger it
      throw new IOException(e.toString(), e);
    }
  }

  
  public List<String> splitAclPairs(String aclString, String realm) {
    List<String> list = Lists.newArrayList(
        Splitter.on(',').omitEmptyStrings().trimResults()
                .split(aclString));
    ListIterator<String> listIterator = list.listIterator();
    while (listIterator.hasNext()) {
      String next = listIterator.next();
      if (next.startsWith("sasl") && next.endsWith("@")) {
        listIterator.set(next + realm);
      }
    }
    return list;
  }

  /**
   * Parse a string down to an ID, adding a realm if needed
   * @param idPair id:data tuple
   * @param realm realm to add
   * @return the ID.
   * @throws IllegalArgumentException if the idPair is invalid 
   */
  public Id parse(String idPair, String realm) {
    int firstColon = idPair.indexOf(':');
    int lastColon = idPair.lastIndexOf(':');
    if (firstColon == -1 || lastColon == -1 || firstColon != lastColon) {
      throw new IllegalArgumentException(
          "ACL '" + idPair + "' not of expected form scheme:id");
    }
    String scheme = idPair.substring(0, firstColon);
    String id = idPair.substring(firstColon + 1);
    if (id.endsWith("@")) {
      Preconditions.checkArgument(
          StringUtils.isNotEmpty(realm),
          "@ suffixed account but no realm %s", id);
      id = id + realm;
    }
    return new Id(scheme, id);

  }


  /**
   * Parse the IDs, adding a realm if needed, setting the permissions
   * @param principalList id string
   * @param realm realm to add
   * @param perms permissions
   * @return the relevant ACLs
   */
  public List<ACL> buildACLs(String principalList, String realm, int perms) throws
      IOException {
    List<String> aclPairs = splitAclPairs(principalList, realm);
    List<ACL> ids = new ArrayList<ACL>(aclPairs.size());
    for (String aclPair : aclPairs) {
      ACL newAcl = new ACL();
      newAcl.setId(parse(aclPair, realm));
      newAcl.setPerms(perms);
      ids.add(newAcl);
    }
    return ids;
  }


  /**
   * Parse an ACL list. This includes configuration indirection
   * {@link ZKUtil#resolveConfIndirection(String)}
   * @param zkAclConf configuration string
   * @return an ACL list
   * @throws IOException on a bad ACL parse
   */
  public List<ACL> parseACLs(String zkAclConf) throws IOException {
    try {
      return ZKUtil.parseACLs(ZKUtil.resolveConfIndirection(zkAclConf));
    } catch (ZKUtil.BadAclFormatException e) {
      throw new IOException("Parsing " + zkAclConf + " :" + e, e);
    }
  }
  
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
      + " doNotPrompt=true\n"
      + " storeKey=true;\n"
      + "}; \n";


  public String createJAASEntry(
      String role,
      String principal,
      File keytab) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(principal),
        "invalid principal");
    Preconditions.checkArgument(StringUtils.isNotEmpty(role),
        "invalid role");
    Preconditions.checkArgument(keytab != null && keytab.isFile(),
        "Keytab null or missing: ");
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
    StringBuilder jaasBinding = new StringBuilder(256);
    jaasBinding.append(createJAASEntry("Server", principal, keytab));
    jaasBinding.append(createJAASEntry("Client", principal, keytab));
    FileUtils.write(dest, jaasBinding.toString());
  }

  public static String bindJVMtoJAASFile(File jaasFile) {
    String path = jaasFile.getAbsolutePath();
    LOG.debug("Binding {} to {}", Environment.JAAS_CONF_KEY, path);
    lastSetJAASFile = jaasFile;
    return System.setProperty(Environment.JAAS_CONF_KEY, path);
  }

  public static void bindZKToServerJAASContext(String contextName) {
    System.setProperty(ZK_SASL_SERVER_CONTEXT, contextName);
  }

  /**
   * Reset any system properties related to JAAS
   */
  public static void clearJaasSystemProperties() {
    System.clearProperty(Environment.JAAS_CONF_KEY);
  }

  /**
   * Resolve the context of an entry. This is an effective test of 
   * JAAS setup, because it will relay detected problems up
   * @param context context name
   * @return the entry
   * @throws FileNotFoundException if there is no context entry oifnd
   */
  public static AppConfigurationEntry[] validateContext(String context) throws
      FileNotFoundException {
    javax.security.auth.login.Configuration configuration =
        javax.security.auth.login.Configuration.getConfiguration();
    AppConfigurationEntry[] entries =
        configuration.getAppConfigurationEntry(context);
    if (entries == null) {
      throw new FileNotFoundException(
          String.format("Entry \"%s\" not found; " +
                        "JAAS config = %s",
              context, describeProperty(Environment.JAAS_CONF_KEY) ));
    }
    return entries;
  }

  /**
   * Set the client properties. This forces the ZK client into 
   * failing if it can't auth.
   * <b>Important:</b>This is JVM-wide.
   * @param username username
   * @param context login context
   * @throws RuntimeException if the context cannot be found in the current
   * JAAS context
   */
  public static void setZKSaslClientProperties(String username,
      String context) throws FileNotFoundException {
    RegistrySecurity.validateContext(context);
    enableZookeeperSASL();
    System.setProperty(ZK_SASL_CLIENT_USERNAME, username);
    System.setProperty(LOGIN_CONTEXT_NAME_KEY, context);
    bindZKToServerJAASContext(context);
  }

  /**
   * Turn ZK SASL on 
   * <b>Important:</b>This is JVM-wide
   */
  protected static void enableZookeeperSASL() {
    System.setProperty(ENABLE_CLIENT_SASL_KEY, "true");
  }

  /**
   * Clear all the ZK Sasl properties
   * <b>Important:</b>This is JVM-wide
   */
  public static void clearZKSaslProperties() {
    disableZookeeperSASL();
    System.clearProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY);
    System.clearProperty(ZookeeperConfigOptions.ZK_SASL_CLIENT_USERNAME);
  }

  /**
   * Force disable ZK SASL bindings.
   * <b>Important:</b>This is JVM-wide
   */
  public static void disableZookeeperSASL() {
    System.clearProperty(ZooKeeperSaslClient.ENABLE_CLIENT_SASL_KEY);
  }

  /**
   * Log details about the current Hadoop user at INFO.
   * Robust against IOEs when trying to get the current user
   */
  public void logCurrentHadoopUser() {
    try {
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      LOG.info("Current user = {}",currentUser);
      UserGroupInformation realUser = currentUser.getRealUser();
      LOG.info("Real User = {}" , realUser);
    } catch (IOException e) {
      LOG.warn("Failed to get current user {}, {}", e);
    }
  }

  public ACL buildOwnerWriteACL(String principal) {
    // this is oly valid on a secure cluster
    // todo
    return null;
  }
  
  /**
   * Stringify a list of ACLs for logging
   * @param acls ACL list
   * @return a string for logs, exceptions, ...
   */
  public static String aclsToString(List<ACL> acls) {
    StringBuilder builder = new StringBuilder();
    if (acls == null) {
      builder.append("null ACL");
    } else {
      builder.append('\n');
      for (ACL acl1 : acls) {
        builder.append(acl1.toString()).append(" ");
      }
    }
    return builder.toString();
  }

  /**
   * Build up low-level security diagnostics to aid debugging
   * @return a string to use in diagnostics
   */
  public String buildSecurityDiagnostics() {
    StringBuilder builder = new StringBuilder();
    builder.append(describeProperty(Environment.JAAS_CONF_KEY));
    String sasl =
        System.getProperty(ENABLE_CLIENT_SASL_KEY,
            ENABLE_CLIENT_SASL_DEFAULT);
    boolean saslEnabled = Boolean.valueOf(sasl);
    builder.append(describeProperty(ENABLE_CLIENT_SASL_KEY,
        ENABLE_CLIENT_SASL_DEFAULT));
    if (saslEnabled) {
      builder.append(describeProperty(ZK_SASL_CLIENT_USERNAME));
      builder.append(describeProperty(LOGIN_CONTEXT_NAME_KEY));
    }
    builder.append(describeProperty(ZK_ALLOW_FAILED_SASL_CLIENTS,
        "(undefined but defaults to true)"));
    builder.append(describeProperty(ZK_MAINTAIN_CONNECTION_DESPITE_SASL_FAILURE));

    return builder.toString();
  }


  private static String describeProperty(String name) {
    return describeProperty(name, "(undefined)");
  }
  
  private static String describeProperty(String name, String def) {
    return "; " + name + "=" + System.getProperty(name, def);
  }

  /**
   * Get the default kerberos realm —returning "" if there
   * is no realm or other problem
   * @return the default realm of the system if it
   * could be determined
   */
  public static String getDefaultRealm() {
    try {
      return KerberosUtil.getDefaultRealm();
      // JDK7
    } catch (ClassNotFoundException e) {

    } catch (NoSuchMethodException e) {

    } catch (IllegalAccessException e) {

    } catch (InvocationTargetException e) {

    }
    return "";
  }
  /**
   * Build the ACLs for the current user
   * @return
   */
/*
  public String buildRWACLsForCurrentPrincipal() {
    
  }
*/

  /**
   * Build the base ACL for a user
   * @param principal the principal
   * @return
   */
/*
  public String buildUserBaseACLForUser(String principal) {
    
    
  }
*/

}
