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

import static org.apache.hadoop.yarn.registry.client.api.RegistryConstants.*;

/**
 * Methods for binding paths according to recommended layout
 */
public class BindingUtils {

  /**
   * Buld the user path -switches to the system path if the user is ""
   * @param user username or ""
   * @return the path to the user
   */
  public static String userPath(String user) {
    if (user.isEmpty()) {
      return PATH_SYSTEM_SERVICES_PATH;
    }
    return PATH_USERS + user;
  }

  public static String serviceclassPath(String user,
      String serviceClass) {

    return userPath(user) + "/" +
           serviceClass;
  }

  public static String servicePath(String user,
      String serviceClass,
      String serviceName) {

    return serviceclassPath(user, serviceClass)
           + "/" + serviceName;
  }

  public static String componentListPath(String user,
      String serviceClass, String serviceName) {

    return servicePath(user, serviceClass, serviceName) + SUBPATH_COMPONENTS;
  }

  public static String componentPath(String user,
      String serviceClass, String serviceName, String componentName) {

    return componentListPath(user, serviceClass, serviceName)
           + "/" +
           componentName;
  } 


}
