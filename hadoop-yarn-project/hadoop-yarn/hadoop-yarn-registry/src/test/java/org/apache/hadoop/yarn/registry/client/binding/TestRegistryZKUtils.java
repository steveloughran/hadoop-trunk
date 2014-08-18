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

import org.apache.hadoop.yarn.registry.client.binding.RegistryZKUtils;
import org.apache.hadoop.yarn.registry.client.exceptions.RESTIOException;
import org.junit.Assert;
import org.junit.Test;

public class TestRegistryZKUtils extends Assert {
  
  @Test
  public void testPaths() throws Throwable {
    assertCreatedPathEquals("/", "/", "");
    assertCreatedPathEquals("/", "", "");
    assertCreatedPathEquals("/", "", "/");

    assertCreatedPathEquals("/a", "/a", "");
    assertCreatedPathEquals("/a", "/", "a");
    assertCreatedPathEquals("/a/b", "/a", "b");
    assertCreatedPathEquals("/a/b", "/a/", "b");
    assertCreatedPathEquals("/a/b", "/a", "/b");
    assertCreatedPathEquals("/a/b", "/a", "/b/");
    assertCreatedPathEquals("/a", "/a", "/");
  }

  @Test
  public void testComplexPaths() throws Throwable {
    assertCreatedPathEquals("/", "", "");
    assertCreatedPathEquals("/yarn/registry/users/hadoop/org-apache-hadoop",
        "/yarn/registry",
        "users/hadoop/org-apache-hadoop/");
  }


  private static void assertCreatedPathEquals(String expected, String base,
      String path) throws RESTIOException {
    String fullPath = RegistryZKUtils.createFullPath(base, path);
    assertEquals("\"" + base + "\" + \"" + path + "\" =\"" + fullPath + "\"",
        expected, fullPath);

  }
}
