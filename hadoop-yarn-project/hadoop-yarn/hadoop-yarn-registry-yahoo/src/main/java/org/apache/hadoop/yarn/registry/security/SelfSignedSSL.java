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
package org.apache.hadoop.yarn.registry.security;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.UUID;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.yarn.registry.proto.SSLProtos.SelfSignedSSLProto;
import org.apache.hadoop.yarn.registry.webapp.Utils;
import org.apache.hadoop.util.Shell;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.map.ObjectMapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Stores Self Signed SSL Certificates
 */
public class SelfSignedSSL extends SecurityData {
  private static final Log LOG = LogFactory.getLog(SelfSignedSSL.class);

  @Override
  public String getName() {
    return "SelfSignedSSL";
  }

  private static void runCommand(String ... cmd) throws IOException {
    Shell.ShellCommandExecutor exec = new Shell.ShellCommandExecutor(cmd, null, null, 15000);
    try {
      exec.execute();
    } catch (Shell.ExitCodeException e) {
      LOG.warn("Error running command " + exec + 
               "\nEXIT CODE:" + exec.getExitCode() + 
               "\nOUT" + exec.getOutput(), e);
      throw e;
    }
  }


  private static String slurpASCII(File f) throws IOException {
    return new String(slurp(f), "ASCII");
  }

  private static byte[] slurp(File f) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    FileInputStream input = new FileInputStream(f);
    try {
      byte [] buffer = new byte[4096];
      int read = 0;
      while ((read = input.read(buffer)) >= 0) {
        out.write(buffer, 0, read);
      }
    } finally {
      input.close();
    }
    return out.toByteArray();
  }

  @Override
  public byte [] validateAndPrepareForStorage(JsonNode securityParams) throws SecurityDataException {
    String dname = Utils.getString(securityParams, true, "dname");
    String password = Utils.getString(securityParams, true, "password");
    Integer validity = Utils.getInt(securityParams, true, "validityDays");
    String alias = Utils.getString(securityParams, true, "alias");
    if (dname == null) {
      dname = "CN=apache, OU=yarn, O=registry";
    }
    if (alias == null) {
      alias = "selfsigned";
    }
    if (password == null) {
      password = UUID.randomUUID().toString();
    }
    if (validity == null) {
      validity = (365 * 2); //2 years by default
    }
    File tempPkcs12 = null;
    File tempCert = null;
    try {
      tempPkcs12 = File.createTempFile("keystore",".pkcs12");
      tempCert = File.createTempFile("keystore",".cert");

      //TODO it would be good to not rely on keytool being on the PATH
      //TODO it would be great to generate this a different way, but the APIs used are sun specific
      //We probably want to investigate bouncy house for the key generation.

      tempPkcs12.delete(); //keytool complains if the keystore is empty
      runCommand("keytool", "-genkey", "-keyalg", "RSA", "-alias", alias,
                 "-keystore", tempPkcs12.getAbsolutePath(),
                 "-storetype", "PKCS12", "-storepass", password,
                 "-validity", validity.toString(), "-keysize", "2048",
                 "-dname", dname);
      byte [] pkcs12 = slurp(tempPkcs12);

      tempCert.delete();
      runCommand("keytool", "-export", "-alias", alias,
                 "-file", tempCert.getAbsolutePath(), "-storetype", "PKCS12",
                 "-storepass", password, "-keystore", tempPkcs12.getAbsolutePath(), "-rfc");
      String cert = slurpASCII(tempCert);

      SelfSignedSSLProto ret = SelfSignedSSLProto.newBuilder()
        .setPassword(password)
        .setAlias(alias)
        .setPkcs12Store(ByteString.copyFrom(pkcs12))
        .setCert(cert)
        .build();
      return ret.toByteArray();
    } catch (IOException e) {
      throw new SecurityDataException("Error while trying to generate Self Signed SSL Certificate", e);
    } finally {
      if (tempCert != null && !tempCert.delete()) {
        LOG.warn("Could not delete temp certificate for some reason "+tempCert);
      }
      if (tempPkcs12 != null && !tempPkcs12.delete()) {
        LOG.warn("Could not delete temp keystore for some reason "+tempPkcs12);
      }
    }
  }

  @Override
  public JsonNode getPublicInformation(byte [] data) throws SecurityDataException {
    return getInformation(data, false);
  }

  @Override
  public JsonNode getAllInformation(byte [] data) throws SecurityDataException {
    return getInformation(data, true);
  }

  private static JsonNode getInformation(byte [] data, boolean includePrivate) throws SecurityDataException { 
    try {
      SelfSignedSSLProto proto = SelfSignedSSLProto.parseFrom(data);
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode rootNode = mapper.createObjectNode();
      if (includePrivate) {
        rootNode.put("password",proto.getPassword());
        rootNode.put("alias",proto.getAlias());
        rootNode.put("pkcs12",proto.getPkcs12Store().toByteArray());
      }
      rootNode.put("cert", proto.getCert());
      return rootNode;
    } catch (InvalidProtocolBufferException e) {
      throw new SecurityDataException("Error trying to parse stored security data.",e);
    }
  }

  public static String getCert(byte [] data) throws SecurityDataException {
    try {
      SelfSignedSSLProto proto = SelfSignedSSLProto.parseFrom(data);
      return proto.getCert();
    } catch (InvalidProtocolBufferException e) {
      throw new SecurityDataException("Error trying to parse stored security data.",e);
    }
  }
}
