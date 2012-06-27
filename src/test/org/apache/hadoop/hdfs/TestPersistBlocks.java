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

package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.IOUtils;

import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

/**
 * A JUnit test for checking if restarting DFS preserves the
 * blocks that are part of an unclosed file.
 */

public class TestPersistBlocks {
  static Log LOG = LogFactory.getLog(TestPersistBlocks.class);

  static final byte[] DATA_BEFORE_RESTART = new byte[4096 * 5];
  static final byte[] DATA_AFTER_RESTART = new byte[4096 * 5];
  static {
    Random rand = new Random();
    rand.nextBytes(DATA_BEFORE_RESTART);
    rand.nextBytes(DATA_AFTER_RESTART);
  }
  
  /** check if DFS remains in proper condition after a restart */
  @Test
  public void testRestartDFS() throws Exception {
    final Configuration conf = new Configuration();
    // Turn off persistent IPC, so that the DFSClient can survive NN restart
    //conf.setInt(     CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY, 0);
    conf.setBoolean(DFSConfigKeys.DFS_PERSIST_BLOCKS_KEY, true);
    MiniDFSCluster cluster = null;
    try {
      FileSystem fs = null;
  
      final String file = "/data";
      final Path dataPath = new Path(file);
  
      long len = 0;
      FSDataOutputStream stream;
  
      conf.setBoolean(DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_ENABLED_KEY, true);
      conf.setBoolean(DFSConfigKeys.DFS_SUPPORT_APPEND_KEY, true);
      
      // disable safemode extension to make the test run faster.
      conf.set("dfs.safemode.extension", "1");
      cluster = new MiniDFSCluster(conf, 4, true, null);
      cluster.waitActive();
      
      fs = cluster.getFileSystem();
      // Creating a file with 4096 blockSize to write multiple blocks
      stream = fs.create(dataPath, true, 4096, (short) 1, 4096);
      stream.write(DATA_BEFORE_RESTART);
      stream.flush();
      
      // Wait for at least a few blocks to get through
      while (len <= 4096) {
        FileStatus status = fs.getFileStatus(dataPath);
        len = status.getLen();
        Thread.sleep(100);
      }


      cluster.shutdownNameNode();
      LOG.info("Shutdown NameNode");
      
      

      cluster.restartNameNode(false, true);
      NameNode namenode = cluster.getNameNode();
      namenode.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
      cluster.waitActive();
      
      LOG.info("Restarted NameNode");
      // Here we restart the cluster without formatting namenode

      
      // Check that the file has no less bytes than before the restart
      // This would mean that blocks were successfully persisted to the log
      FileStatus status = fs.getFileStatus(dataPath);
      Assert.assertTrue("Length too short: " + status.getLen(),
          status.getLen() >= len);
      
      // And keep writing (ensures that leases are also persisted correctly)
      stream.write(DATA_AFTER_RESTART);
      stream.close();

      // Verify that the data showed up, both from before and after the restart.
      FSDataInputStream readStream = fs.open(dataPath);
      try {
        byte[] verifyBuf = new byte[DATA_BEFORE_RESTART.length];
        IOUtils.readFully(readStream, verifyBuf, 0, verifyBuf.length);
        Assert.assertArrayEquals(DATA_BEFORE_RESTART, verifyBuf);
        
        IOUtils.readFully(readStream, verifyBuf, 0, verifyBuf.length);
        Assert.assertArrayEquals(DATA_AFTER_RESTART, verifyBuf);
      } finally {
        IOUtils.closeStream(readStream);
      }
      
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
}
