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

package org.apache.hadoop.yarn.registry.client.binding;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.api.RegistryOperations;
import org.apache.hadoop.yarn.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import static org.apache.hadoop.yarn.registry.client.binding.RegistryPathUtils.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Utility methods for working with a registry via a registry operations
 * instance.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RegistryOperationUtils {

  public static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";

  /**
   * Buld the user path -switches to the system path if the user is "".
   * It also cross-converts the username to ascii via punycode
   * @param shortname username or ""
   * @return the path to the user
   */
  public static String homePathForUser(String shortname) {
    Preconditions.checkArgument(shortname != null, "null user");

    // catch recursion
    if (shortname.startsWith(RegistryConstants.PATH_USERS)) {
      return shortname;
    }
    if (shortname.isEmpty()) {
      return RegistryConstants.PATH_SYSTEM_SERVICES;
    }
    return RegistryPathUtils.join(RegistryConstants.PATH_USERS,
        encodeForRegistry(shortname));
  }

  /**
   * Create a service classpath
   * @param user username or ""
   * @param serviceClass service name
   * @return a full path
   */
  public static String serviceclassPath(String user,
      String serviceClass) {
    String services = join(homePathForUser(user),
        RegistryConstants.PATH_USER_SERVICES);
    return join(services,
        serviceClass);
  }

  /**
   * Create a path to a service under a user & service class
   * @param user username or ""
   * @param serviceClass service name
   * @param serviceName service name unique for that user & service class
   * @return a full path
   */
  public static String servicePath(String user,
      String serviceClass,
      String serviceName) {

    return join(
        serviceclassPath(user, serviceClass),
        serviceName);
  }

  /**
   * Create a path for listing components under a service
   * @param user username or ""
   * @param serviceClass service name
   * @param serviceName service name unique for that user & service class
   * @return a full path
   */
  public static String componentListPath(String user,
      String serviceClass, String serviceName) {

    return join(servicePath(user, serviceClass, serviceName),
        RegistryConstants.SUBPATH_COMPONENTS);
  }

  /**
   * Create the path to a service record for a component
   * @param user username or ""
   * @param serviceClass service name
   * @param serviceName service name unique for that user & service class
   * @param componentName unique name/ID of the component
   * @return a full path
   */
  public static String componentPath(String user,
      String serviceClass, String serviceName, String componentName) {

    return join(
        componentListPath(user, serviceClass, serviceName),
        componentName);
  }

  /**
   * List service records directly under a path
   * @param registryOperations registry operations instance
   * @param path path to list
   * @return a mapping of the service records that were resolved, indexed
   * by their name under the supplied path
   * @throws IOException
   */
  public static Map<String, ServiceRecord> listServiceRecords(
      RegistryOperations registryOperations,
      String path) throws IOException {
    List<RegistryPathStatus> stats = registryOperations.listFull(path);
    return RecordOperations.extractServiceRecords(registryOperations, stats);
  }

  /**
   * Get the home path of the current user.
   * <p>
   *  In an insecure cluster, the environment variable 
   *  {@link #HADOOP_USER_NAME} is queried <i>first</i>.
   * <p>
   * This means that in a YARN container where the creator set this 
   * environment variable to propagate their identity, the defined
   * user name is used in preference to the actual user.
   * <p>
   * In a secure cluster, the kerberos identity of the current user is used.
   * @return a path for the current user's home dir.
   * @throws RuntimeException if the current user identity cannot be determined
   * from the OS/kerberos.
   */
  public static String homePathForCurrentUser() {
    String shortUserName = null;
    if (!UserGroupInformation.isSecurityEnabled()) {
      shortUserName = System.getenv(HADOOP_USER_NAME);
    }
    if (StringUtils.isEmpty(shortUserName)) {
      try {
        shortUserName = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return homePathForUser(shortUserName);
  }

  /**
   * Get the current user path formatted for the registry
   * <p>
   *  In an insecure cluster, the environment variable 
   *  {@link #HADOOP_USER_NAME} is queried <i>first</i>.
   * <p>
   * This means that in a YARN container where the creator set this 
   * environment variable to propagate their identity, the defined
   * user name is used in preference to the actual user.
   * <p>
   * In a secure cluster, the kerberos identity of the current user is used.
   * @return the encoded shortname of the current user
   * @throws RuntimeException if the current user identity cannot be determined
   * from the OS/kerberos.
   * 
   */
  public static String currentUser() {
    String shortUserName = null;
    if (!UserGroupInformation.isSecurityEnabled()) {
      shortUserName = System.getenv(HADOOP_USER_NAME);
    }
    if (StringUtils.isEmpty(shortUserName)) {
      try {
        shortUserName =
            UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return encodeForRegistry(shortUserName);
  }

}
