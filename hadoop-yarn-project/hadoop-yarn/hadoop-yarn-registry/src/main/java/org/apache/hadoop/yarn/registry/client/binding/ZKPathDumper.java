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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * This class dumps a registry tree to a string.
 * It does this in the toString() method, so it
 * can be used in a log statement -the operation
 * will only take place if the method is evaluated.
 * 

 */
public class ZKPathDumper {

  private final CuratorFramework curator;
  private final String root;
  private final boolean verbose = true;

  /**
   * Create a path dumper -but do not dump the path until asked
   * @param curator curator instance
   * @param root root
   */
  public ZKPathDumper(CuratorFramework curator,
      String root) {
    Preconditions.checkArgument(curator != null);
    Preconditions.checkArgument(root != null);
    this.curator = curator;
    this.root = root;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("ZK tree for ").append(root).append('\n');
    expand(builder, root, 1);
    return builder.toString();
  }

  private void expand(StringBuilder builder,
      String path,
      int indent) {
    try {
      GetChildrenBuilder childrenBuilder = curator.getChildren();
      List<String> children = childrenBuilder.forPath(path);
      for (String child : children) {
        String childPath = path + "/" + child;
        String body = "";
        Stat stat = curator.checkExists().forPath(childPath);
        StringBuilder verboseDataBuilder = new StringBuilder(64);
        verboseDataBuilder.append("  [")
                          .append(stat.getDataLength())
                          .append("]");
        if (stat.getEphemeralOwner() > 0) {
          verboseDataBuilder.append("*");
        }
        body = verboseDataBuilder.toString();

        // print each child
        append(builder, indent, ' ');
        builder.append('/').append(child);
        builder.append(body);
        builder.append('\n');
        // recurse
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
