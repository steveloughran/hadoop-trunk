/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift.util;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Various utility classes for SwiftFS support
 */
public final class SwiftUtils {

  /**
   * Join two (non null) paths, inserting a forward slash between them
   * if needed
   *
   * @param path1 first path
   * @param path2 second path
   * @return the combined path
   */
  public static String joinPaths(String path1, String path2) {
    StringBuilder result =
            new StringBuilder(path1.length() + path2.length() + 1);
    result.append(path1);
    boolean insertSlash = true;
    if (path1.endsWith("/")) {
      insertSlash = false;
    } else if (path2.startsWith("/")) {
      insertSlash = false;
    }
    if (insertSlash) {
      result.append("/");
    }
    result.append(path2);
    return result.toString();
  }

  /**
   * This test contains the is-directory logic for Swift, so if
   * changed there is only one place for it.
   *
   * @param fileStatus status to examine
   * @return true if we consider this status to be representative of a
   *         directory.
   */
  public static boolean isDirectory(FileStatus fileStatus) {
    return fileStatus.isDir() || isFilePretendingToBeDirectory(fileStatus);
  }

  /**
   * Test for the entry being a file that is treated as if it is a
   * directory
   *
   * @param fileStatus status
   * @return true if it meets the rules for being a directory
   */
  public static boolean isFilePretendingToBeDirectory(FileStatus fileStatus) {
    return fileStatus.getLen() == 0;
  }

  public static boolean isRootDir(SwiftObjectPath swiftObject) {
    return swiftObject.objectMatches("") || swiftObject.objectMatches("/");
  }

  public static void debug(Log log, String text, Object... args) {
    if (log.isDebugEnabled()) {
      log.debug(String.format(text, args));
    }
  }

  public static void trace(Log log, String text, Object... args) {
    if (log.isTraceEnabled()) {
      log.trace(String.format(text, args));
    }
  }

  /**
   * Given a partition number, calculate the partition value.
   * This is used in the SwiftNativeOutputStream, and is placed
   * here for tests to be able to calculate the filename of
   * a partition.
   * @param partNumber part number
   * @return a string to use as the filename
   */
  public static String partitionFilenameFromNumber(int partNumber) {
    return String.format("%06d", partNumber);
  }

  public static String ls(FileSystem fileSystem, Path path) throws
                                                            IOException {
    if (path == null) {
      //surfaces when someone calls getParent() on something at the top of the path
      return "/";
    }
    FileStatus[] stats;
    String pathtext = "ls " + path;
    try {
      stats = fileSystem.listStatus(path);
    } catch (FileNotFoundException e) {
      return pathtext + " -file not found";
    } catch (IOException e) {
      return pathtext + " -failed: " + e;
    }
    return pathtext + fileStatsToString(stats, "\n");
  }

  /**
   * Take an array of filestats and convert to a string (prefixed w/ a [01] counter
   * @param stats array of stats
   * @param separator separator after every entry
   * @return a stringified set
   */
  public static String fileStatsToString(FileStatus[] stats, String separator) {
    StringBuilder buf = new StringBuilder(stats.length * 128);
    for (int i = 0; i < stats.length; i++) {
      buf.append(String.format("[%02d] %s", i, stats[i])).append(separator);
    }
    return buf.toString();
  }
  
}
