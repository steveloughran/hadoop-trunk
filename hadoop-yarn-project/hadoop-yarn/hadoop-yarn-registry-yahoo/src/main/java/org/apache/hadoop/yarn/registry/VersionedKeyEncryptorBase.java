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
package org.apache.hadoop.yarn.registry;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.yarn.registry.proto.VersionedEncProtos.VersionedDataProto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Interface that all encryption methods should support. 
 */
@Public
@Evolving
public abstract class VersionedKeyEncryptorBase implements VersionedKeyEncryptor {
  private Configuration conf; 

  public static class VersionedBytes {
    private long version;
    private byte[] bytes;

    public VersionedBytes(long version, byte[] bytes) {
      this.version = version;
      this.bytes = bytes;
    }

    public long getVersion() {
      return version;
    }

    public byte[] getBytes() {
      return bytes;
    }
  }

  @Override
  public void init(Configuration conf) throws Exception {
    this.conf = conf;
  }

  public Configuration getConf() {
    return conf;
  }

  @Override
  public void start() throws Exception {
    //Empty
  }

  @Override
  public void stop() throws Exception {
    //Empty
  }

  /**
   * Encrypt the data, but also return the version of the key that was used.
   * @param data the data to encrypt
   * @return the encrypted bytes and the version used to encrypt them.
   */ 
  public abstract VersionedBytes versionedEncrypt(byte[] data) throws RegistryException;

  /**
   * Decrypt the data with a specific version of the key.
   * @param version the version of the key to use.
   * @param data the data to decrypt.
   * @return teh decrypted bytes.
   */ 
  public abstract byte[] versionedDecrypt(long version, byte[] data) throws RegistryException;

  @Override
  public byte[] encrypt(byte[] data) throws RegistryException {
    VersionedBytes vb = versionedEncrypt(data);
    return VersionedDataProto.newBuilder()
              .setVersion(vb.getVersion())
              .setData(ByteString.copyFrom(vb.getBytes()))
              .build().toByteArray();
  }

  @Override
  public byte[] decrypt(byte[] data) throws RegistryException {
    try {
      VersionedDataProto proto = VersionedDataProto.parseFrom(data);
      return versionedDecrypt(proto.getVersion(), proto.getData().toByteArray());
    } catch (InvalidProtocolBufferException e) {
      throw new RegistryException("Could not decrypt the secure data",500, e);
    }
  }

  @Override
  public byte[] recrypt(byte [] data) throws RegistryException {
    return encrypt(decrypt(data));
  }
}

