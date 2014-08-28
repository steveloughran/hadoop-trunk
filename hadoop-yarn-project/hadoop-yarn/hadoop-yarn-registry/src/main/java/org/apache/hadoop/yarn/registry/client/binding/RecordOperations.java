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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.api.RegistryOperations;
import org.apache.hadoop.yarn.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.yarn.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Support for operations on records
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RecordOperations {
  private static final Logger LOG = LoggerFactory.getLogger(JsonSerDeser.class);

  public static class ServiceRecordMarshal extends JsonSerDeser<ServiceRecord> {
    public ServiceRecordMarshal() {
      super(ServiceRecord.class, RegistryConstants.RECORD_HEADER);
    }
  }

  /**
   * Extract all service records under a list of stat operations...this
   * skips entries that are too short or simply not matching
   * @param operations operation support for fetches
   * @param stats list of stat results
   * @return a possibly empty list
   * @throws IOException for any IO Operation that wasn't ignored.
   */
  public static List<ServiceRecord> extractServiceRecords(RegistryOperations operations,
      RegistryPathStatus[] stats) throws IOException {
    List<ServiceRecord> results = new ArrayList<ServiceRecord>(stats.length);
    for (RegistryPathStatus stat : stats) {
      if (stat.size > RegistryConstants.RECORD_HEADER.length) {
        // maybe has data
        try {
          ServiceRecord serviceRecord = operations.resolve(stat.path);
          results.add(serviceRecord);
        } catch (EOFException ignored) {
          LOG.debug("data too short for {}", stat.path);
        } catch (InvalidRecordException record) {
          LOG.debug("Invalid record at {}", stat.path);
        }

      }
    }
    return results;

  }


}
