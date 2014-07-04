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
package org.apache.hadoop.yarn.registry.integration;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import javax.xml.bind.DatatypeConverter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.codehaus.jackson.JsonNode;

import org.apache.hadoop.yarn.registry.webapp.Utils;

public class TestIntegration extends Integration {
  private static final Log LOG = LogFactory.getLog(TestIntegration.class);

  @Before
  public void before() throws Exception {
    startServer(null);
  }

  @After
  public void after() throws Exception {
    stopServer();
  }

  @Test
  public void testBasic() throws Exception {
    //Insecure virtual hosts
    assertJsonGet("virtualHost/test", "{\"error\":{\"description\":\"test is not a registered virtual host\"}}", 404);
    assertJsonPost("virtualHost", "{\"virtualHost\":{\"name\":\"test\"}}");
    assertJsonGet("virtualHost/test", "{\"virtualHost\":{\"name\":\"test\", \"serverTimeoutSecs\":600}}");
    assertJsonPut("virtualHost/test/server/server1", "{\"server\":{\"host\": \"myhost\"}}");
    assertJsonGet("virtualHost/test/server/server1", "{\"server\":{\"serverId\":\"server1\", \"host\": \"myhost\"}}", false); //Ignoring the heartbeat
    assertJsonGet("virtualHost/test/server", "{\"server\":[{\"serverId\":\"server1\", \"host\": \"myhost\"}]}", false); //Ignoring the heartbeat time
    get("virtualHost/test/SelfSignedSSL.cert", 404, null);

    //Secure virtual host
    assertJsonPost("virtualHost", "{\"virtualHost\":{\"name\":\"ssltest\", \"owner\":[\"user:me\"], \"securityData\":{\"SelfSignedSSL\":{}}}}", "me");
    assertJsonGet("virtualHost/ssltest", "{\"virtualHost\":{\"name\":\"ssltest\",\"owner\":[\"user:me\"],\"serverTimeoutSecs\":600,\"securityData\":{\"SelfSignedSSL\":{}}}}", false);
    assertJsonGet("virtualHost/ssltest", "{\"virtualHost\":{\"name\":\"ssltest\",\"owner\":[\"user:me\"],\"serverTimeoutSecs\":600,\"securityData\":{\"SelfSignedSSL\":{}}}}", false, "me");
    assertJsonPut("virtualHost/ssltest/server/server1", "{\"server\":{\"host\": \"myhost\"}}", "me");
    assertJsonGet("virtualHost/ssltest/server/server1", "{\"server\":{\"serverId\":\"server1\", \"host\": \"myhost\"}}", false); //Ignoring the heartbeat
    assertJsonGet("virtualHost/ssltest/server", "{\"server\":[{\"serverId\":\"server1\", \"host\": \"myhost\"}]}", false); //Ignoring the heartbeat time
    assertNotNull(get("virtualHost/ssltest/SelfSignedSSL.cert", 200, null));
    
    get("virtualHost/missing/SelfSignedSSL.cert", 404, null);

    String jsonBody = get("virtualHost/ssltest", 200, "me");
    JsonNode root = Utils.parseJson(jsonBody);
    JsonNode ssl = Utils.getNode(root, false, "virtualHost", "securityData", "SelfSignedSSL");
    String password = Utils.getString(ssl, false, "password");
    String alias = Utils.getString(ssl, false, "alias");
    String pkcs12 = Utils.getString(ssl, false, "pkcs12");

    KeyStore ks = KeyStore.getInstance("PKCS12");
    ByteArrayInputStream in = new ByteArrayInputStream(DatatypeConverter.parseBase64Binary(pkcs12));
    ks.load(in, password.toCharArray());
    assertNotNull(ks.getEntry(alias, new KeyStore.PasswordProtection(password.toCharArray())));


    String strCert = Utils.getString(ssl, false, "cert");
    KeyStore trustStore  = KeyStore.getInstance(KeyStore.getDefaultType());
    trustStore.load(null);
    ByteArrayInputStream bis = new ByteArrayInputStream(strCert.getBytes("ASCII"));

    CertificateFactory cf = CertificateFactory.getInstance("X.509");

    Certificate cert = cf.generateCertificate(bis);
    trustStore.setCertificateEntry("something", cert);

    //Errors
    assertJsonPost("virtualHost", "{\"virtualHost\":{\"name\":\"test-2\", \"owner\":[\"user:me\"]}}",
                  "{\"error\":{\"description\":\"Cannot create a non-secure virtual host with a list of owners\"}}", 400, "me");
    assertJsonPost("virtualHost", "{\"virtualHost\":{\"name\":\"test-2\", \"securityData\":{\"SelfSignedSSL\":{}}}}",
                  "{\"error\":{\"description\":\"Cannot create a secure virtual host without a list of owners\"}}", 400, "me");
    assertJsonPost("virtualHost", "{\"virtualHost\":{\"name\":\"test-2\", \"owner\":[\"user:me\"], \"securityData\":{\"SelfSignedSSL\":{}}}}",
                  "{\"error\":{\"description\":\"Attempting to create a secure virtual host without being the owner\"}}", 403, "not-me");
    assertJsonPut("virtualHost/ssltest/server/server1", "{\"server\":{\"host\": \"myhost\"}}", 403, "not-me");
  }
}

