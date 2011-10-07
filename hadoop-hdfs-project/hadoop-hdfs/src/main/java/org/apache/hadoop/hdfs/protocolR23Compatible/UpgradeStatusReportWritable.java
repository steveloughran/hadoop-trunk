/**
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
package org.apache.hadoop.hdfs.protocolR23Compatible;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * Base upgrade upgradeStatus class.
 * 
 * Describes status of current upgrade.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class UpgradeStatusReportWritable implements Writable {
  protected int version;
  protected short upgradeStatus;
  protected boolean finalized;
  
  public static UpgradeStatusReportWritable convert(
      org.apache.hadoop.hdfs.server.common.UpgradeStatusReport r) {
    if (r == null) return null;
    return new UpgradeStatusReportWritable(
        r.getVersion(), r.getUpgradeStatus(), r.isFinalized());
  }
  
  public static org.apache.hadoop.hdfs.server.common.UpgradeStatusReport 
    convert(UpgradeStatusReportWritable r) {
    if (r == null) return null;
    return new org.apache.hadoop.hdfs.server.common.UpgradeStatusReport(
        r.getVersion(), r.getUpgradeStatus(), r.isFinalized());
  }
  
  public UpgradeStatusReportWritable() {
    this(0, (short)0, false);
  }

  public UpgradeStatusReportWritable(int version, short status, boolean isFinalized) {
    this.version = version;
    this.upgradeStatus = status;
    this.finalized = isFinalized;
  }

  /**
   * Get the layout version of the currently running upgrade.
   * @return layout version
   */
  public int getVersion() {
    return this.version;
  }

  /**
   * Get upgrade upgradeStatus as a percentage of the total upgrade done.
   */ 
  public short getUpgradeStatus() {
    return upgradeStatus;
  }

  /**
   * Is current upgrade finalized.
   * @return true if finalized or false otherwise.
   */
  public boolean isFinalized() {
    return this.finalized;
  }

  /**
   * Get upgradeStatus data as a text for reporting.
   * Should be overloaded for a particular upgrade specific upgradeStatus data.
   * 
   * @param details true if upgradeStatus details need to be included, 
   *                false otherwise
   * @return text
   */
  public String getStatusText(boolean details) {
    return "Upgrade for version " + getVersion() 
            + (upgradeStatus<100 ? 
              " is in progress. Status = " + upgradeStatus + "%" : 
              " has been completed."
              + "\nUpgrade is " + (finalized ? "" : "not ")
              + "finalized.");
  }

  /**
   * Print basic upgradeStatus details.
   */
  @Override
  public String toString() {
    return getStatusText(false);
  }

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (UpgradeStatusReportWritable.class,
       new WritableFactory() {
         public Writable newInstance() { return new UpgradeStatusReportWritable(); }
       });
  }

  
  // Note when upgrade has been finalized then the NN always
  // returns a null as the report. 
  // hence the isFinalized is serialized (ugly)
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.version);
    out.writeShort(this.upgradeStatus);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.version = in.readInt();
    this.upgradeStatus = in.readShort();
  }
}
