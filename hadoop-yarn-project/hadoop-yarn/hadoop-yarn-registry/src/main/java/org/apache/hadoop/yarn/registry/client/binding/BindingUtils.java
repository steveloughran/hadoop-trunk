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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

import static org.apache.hadoop.yarn.registry.client.api.RegistryConstants.*;

/**
 * Methods for binding paths according to recommended layout, and for
 * extracting some of the content
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BindingUtils {

  /**
   * Buld the user path -switches to the system path if the user is "".
   * It also cross-converts the username to ascii via punycode
   * @param user username or ""
   * @return the path to the user
   */
  public static String userPath(String user) {
    Preconditions.checkArgument(user != null, "null user");
    if (user.isEmpty()) {
      return PATH_SYSTEM_SERVICES;
    }

    return RegistryPathUtils.join(PATH_USERS,
        RegistryPathUtils.encodeForRegistry(user));
  }

  /**
   * Create a service classpath
   * @param user username or ""
   * @param serviceClass service name
   * @return a full path
   */
  public static String serviceclassPath(String user,
      String serviceClass) {

    return RegistryPathUtils.join(userPath(user),
        serviceClass);
  }
  

  /**
   * Get the current user path formatted for the system
   * @return
   * @throws IOException
   */
  public static String currentUser() throws IOException {
    return RegistryPathUtils.encodeForRegistry(
        UserGroupInformation.getCurrentUser().getShortUserName());
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

    return RegistryPathUtils.join(
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

    return RegistryPathUtils.join(servicePath(user, serviceClass, serviceName),
                                  SUBPATH_COMPONENTS);
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

    return RegistryPathUtils.join(
        componentListPath(user, serviceClass, serviceName),
        componentName);
  }


}
