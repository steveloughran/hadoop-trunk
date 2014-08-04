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

package org.apache.hadoop.yarn.registry.client.binding.zk;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetChildrenBuilder;

import java.util.List;

/**
 * This class dumps a registry tree to a string.
 * It does this in the toString() method, so it
 * can be used in a log statement -the operation
 * will only take place if the method is evaluated.
 * 
 * It will also catch any exceptions raised in the operation,
 * so the log action will never fail.
 */
public class ZKPathDumper {

  private final CuratorFramework curator;
  private final String registryRoot;

  public ZKPathDumper(CuratorFramework curator, String registryRoot) {
    Preconditions.checkArgument(curator != null);
    Preconditions.checkArgument(registryRoot != null);
    this.curator = curator;
    this.registryRoot = registryRoot;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("ZK tree for ").append(registryRoot).append('\n');
    expand(builder, registryRoot, 1);
    return builder.toString();
  }

  private void expand(StringBuilder builder,
      String path,
      int indent) {
    try {
      GetChildrenBuilder childrenBuilder = curator.getChildren();
      List<String> children = childrenBuilder.forPath(path);
      for (String child : children) {
        // print each child
        append(builder, indent, ' ');
        builder.append('/').append(child).append('\n');
        // recurse
        String childPath = path + "/" + child;
        expand(builder, childPath, indent + 1);
      }
    } catch (Exception e) {
      builder.append(e.toString()).append("\n");
    }

  }

  private void append(StringBuilder builder, int indent, char c) {
    for (int i = 0; i < indent; i++) {
      builder.append(c);
    }
  }
}
