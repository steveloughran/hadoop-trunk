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
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.exceptions.InvalidPathnameException;
import org.apache.zookeeper.common.PathUtils;

import java.io.IOException;
import java.net.IDN;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Low level zookeeper-related operations on paths
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RegistryPathUtils {


  private static final Pattern HOSTNAME =
      Pattern.compile(RegistryConstants.HOSTNAME_PATTERN);

  /**
   * Validate ZK path with the path itself included in
   * the exception text
   * @param path path to validate
   */
  public static String validateZKPath(String path) throws
      InvalidPathnameException {
    try {
      PathUtils.validatePath(path);

    } catch (IllegalArgumentException e) {
      throw new InvalidPathnameException(path,
          "Invalid Path \"" + path + "\" : " + e, e);
    }
    return path;
  }

  /**
   * Validate ZK path with the path itself included in
   * the exception text
   * @param path path to validate
   */
  public static String validateElementsAsDNS(String path) throws
      InvalidPathnameException {
    List<String> splitpath = split(path);
    for (String fragment : splitpath) {
      if (!HOSTNAME.matcher(fragment).matches()) {
        throw new InvalidPathnameException(path,
            "Invalid Path element \"" + fragment + "\"");
      }

    }

    return path;
  }

  /*
 * Create a full path from the registry root and the supplied subdir
 * @param path path of operation
 * @return an absolute path
 * @throws IllegalArgumentException if the path is invalide
 */
  public static String createFullPath(String base, String path) throws
      IOException {
    Preconditions.checkArgument(path != null, "null path");
    Preconditions.checkArgument(base != null, "null path");
    return validateZKPath(join(base, path));
  }

  /**
   * Join two paths, guaranteeing that there will not be exactly
   * one separator between the two, and exactly one at the front
   * of the path. There will be no trailing "/" except for the special
   * case that this is the root path
   * @param base base path
   * @param path second path to add
   * @return a combined path.
   */
  public static String join(String base, String path) {
    Preconditions.checkArgument(path != null, "null path");
    Preconditions.checkArgument(base != null, "null path");
    StringBuilder fullpath = new StringBuilder();

    if (!base.startsWith("/")) {
      fullpath.append('/');
    }
    fullpath.append(base);

    if (!path.startsWith("/") && !fullpath.toString().endsWith("/")) {
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

  /**
   * Get the parent of a path
   * @param path
   * @return the parent path
   * @throws PathNotFoundException if the path was at root.
   */
  public static String parentOf(String path) throws PathNotFoundException {
    List<String> elements = split(path);

    int size = elements.size();
    if (size == 0) {
      throw new PathNotFoundException("No parent of " + path);
    }
    if (size == 1) {
      return "/";
    }
    elements.remove(size - 1);
    StringBuilder parent = new StringBuilder(path.length());
    for (String element : elements) {
      parent.append("/");
      parent.append(element);
    }
    return parent.toString();
  }

  /**
   * Perform any formatting for the registry needed to convert
   * non-simple-DNS elements 
   * @param element
   * @return
   */
  public static String encodeForRegistry(String element) {
    return IDN.toASCII(element);
  }
}