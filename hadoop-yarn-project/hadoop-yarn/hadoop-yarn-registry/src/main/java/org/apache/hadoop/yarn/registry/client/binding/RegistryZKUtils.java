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
import org.apache.hadoop.yarn.registry.client.exceptions.InvalidPathnameException;
import org.apache.zookeeper.common.PathUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RegistryZKUtils {

  /**
   * Validate ZK path with the path itself included in
   * the exception text
   * @param path path to validate
   */
  public static String validateZKPath(String path) throws
      InvalidPathnameException {
    try {
      PathUtils.validatePath(path);
      return path;
    } catch (IllegalArgumentException e) {
      throw new InvalidPathnameException(
          "Invalid Path \"" + path + "\" : " + e, e);
    }
  }

  /*
 * Create a full path from the registry root and the supplied subdir
 * @param path path of operation
 * @return an absolute path
 * @throws IllegalArgumentException if the path is invalide
 */
  public static String createFullPath(String base, String path) throws
      IOException {
    String finalpath = join(base, path);
    return validateZKPath(finalpath);
  }

  public static String join(String base, String path) {
    Preconditions.checkArgument(path != null, "null path");
    Preconditions.checkArgument(base != null, "null path");
    StringBuilder fullpath = new StringBuilder();

    if (!base.startsWith("/")) {
      fullpath.append('/');
    }
    fullpath.append(base);

    if (!fullpath.toString().endsWith("/") && !path.startsWith("/")) {
      fullpath.append("/");
    }
    fullpath.append(path);

    //here there may be a trailing "/"
    String finalpath = fullpath.toString();
    if (finalpath.endsWith("/") && !"/".equals(finalpath)) {
      finalpath = finalpath.substring(0, finalpath.length() - 1);

    }
    return finalpath;
  }

  /**
   * split a path into elements, stripping empty elements
   * @param path the path 
   * @return the split path
   */
  public static List<String> split(String path) {
    // 
    String[] pathelements = path.split("/");
    List<String> dirs = new ArrayList<String>(pathelements.length);
    for (String pathelement : pathelements) {
      if (!pathelement.isEmpty()) {
        dirs.add(pathelement);
      }
    }
    return dirs;
  }
}
