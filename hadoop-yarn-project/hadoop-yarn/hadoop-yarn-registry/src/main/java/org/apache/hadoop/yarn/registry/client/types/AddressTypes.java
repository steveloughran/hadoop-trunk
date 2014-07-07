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
 * Enum of address types -as integers. 
 * Why integers and not enums? Cross platform serialization as JSON
 */
public class AddressTypes {

  /**
   * Any other address
   */
  public static final int ADDRESS_OTHER = 0;

  /**
   * URI entries
   */
  public static final int ADDRESS_URI = 1;
  /**
   * hostname:port pairs
   */
  public static final int ADDRESS_HOSTNAME_AND_PORT = 2;
  /**
   * path /a/b/c style
   */
  public static final int ADDRESS_PATH = 3;
}
