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

package org.apache.hadoop.yarn.registry.client.types;


/**
 * Output of a stat() call
 */
public final class RegistryPathStatus {

  /**
   * Path in the registry to this entry
   */
  public final String path; 
  public final long time;
  public final long size;


  public RegistryPathStatus(String path,
      long time,
      long size) {
    this.path = path;
    this.time = time;
    this.size = size;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RegistryPathStatus status = (RegistryPathStatus) o;

    if (size != status.size) {
      return false;
    }
    if (time != status.time) {
      return false;
    }
    if (path != null ? !path.equals(status.path) : status.path != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return path != null ? path.hashCode() : 0;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("RegistryPathStatus{");
    sb.append("path='").append(path).append('\'');
    sb.append(", time=").append(time);
    sb.append(", size=").append(size);
    sb.append('}');
    return sb.toString();
  }
}
