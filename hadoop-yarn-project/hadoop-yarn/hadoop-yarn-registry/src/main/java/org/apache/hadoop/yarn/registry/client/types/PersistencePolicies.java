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
 * Persistence policies for {@link ServiceRecord#persistence}
 */

public interface PersistencePolicies {

  /**
   * The record persists until removed manually: {@value}.
   */
  int PERMANENT = 0;

  /**
   * Remove when the YARN application defined in the id field
   * terminates: {@value}.
   */
  int APPLICATION = 1;

  /**
   * Remove when the current YARN application attempt ID finishes: {@value}.
   */
  int APPLICATION_ATTEMPT = 2;

  /**
   * Remove when the YARN container in the ID field finishes: {@value}
   */
  int CONTAINER = 3;

}
