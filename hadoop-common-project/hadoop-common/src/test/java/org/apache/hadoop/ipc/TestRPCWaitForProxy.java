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
package org.apache.hadoop.ipc;

import junit.framework.Assert;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.delegation.TestDelegationToken;
import org.junit.Test;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetSocketAddress;
import java.net.ConnectException;
import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * tests that the proxy can be interrupted
 */
public class TestRPCWaitForProxy extends Assert {
  private static final String ADDRESS = "0.0.0.0";

  private static final Log LOG =
      LogFactory.getLog(TestRPCWaitForProxy.class);

  private static Configuration conf = new Configuration();

  /**
   * This tests that the time-bounded wait for a proxy operation works, and
   * times out immediately
   * @throws Throwable any exception other than that which was expected
   */
  @Test
  public void testWaitForProxy() throws Throwable {
    RpcThread worker = new RpcThread(0);
    worker.start();
    worker.join();
    Throwable caught = worker.getCaught();
    assertNotNull("No exception was raised from " + worker, caught);
    if (!(caught instanceof ConnectException)) {
      throw caught;
    }
    LOG.debug("Caught "+caught, caught);
  }

  /**
   * This test sets off a blocking thread and then interrupts it, before
   * checking that the thread was interrupted
   * @throws Throwable any exception other than that which was expected
   */
  @Test
  public void testInterruptedWaitForProxy() throws Throwable {
    RpcThread worker = new RpcThread(60000L);
    worker.start();
    worker.interrupt();
    worker.join();
    Throwable caught = worker.getCaught();
    assertNotNull("No exception was raised from " + worker, caught);
    if (!(caught instanceof InterruptedIOException)) {
      throw caught;
    }
    LOG.debug("Caught "+caught, caught);
  }


  /**
   * This thread waits for a proxy for the specified timeout, and retains
   * any throwable that was raised in the process
   */

  private class RpcThread extends Thread {
    private Throwable caught;
    private long timeout;
    protected WaitProtocol protocol;

    private RpcThread(long timeout) {
      this.timeout = timeout;
    }

    @Override
    public void run() {
      try {
        protocol =
            RPC.waitForProxy(WaitProtocol.class,
                             WaitProtocol.versionID,
                             new InetSocketAddress(ADDRESS, 20),
                             conf,
                             timeout);
      } catch (Throwable throwable) {
        caught = throwable;
      }
    }

    public Throwable getCaught() {
      return caught;
    }

    public WaitProtocol getProtocol() {
      return protocol;
    }

    @Override
    public String toString() {
      return "RpcThread with timeout " + timeout + " protocol class "
          +(protocol != null? protocol.getClass() : "(null)")
          +" caught: " + caught;
    }
  }

  private static interface WaitProtocol extends VersionedProtocol {
    public static final long versionID = 1L;
    public long getStartTimeNanos();
  }
  
  private class WaitProtocolImpl implements WaitProtocol {
    @Override
    public long getStartTimeNanos() {
      return 0;
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
        throws IOException {
      if (protocol.equals(WaitProtocol.class.getName()))
        return versionID;
      throw new IOException("Unknown protocol: " + protocol);
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
                                                  long clientVersion,
                                                  int clientMethodsHash)
        throws IOException {
      if (protocol.equals(WaitProtocol.class.getName()))
        return new ProtocolSignature(versionID, null);
      throw new IOException("Unknown protocol: " + protocol);
    }
  }
}
