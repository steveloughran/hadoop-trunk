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
package org.apache.hadoop.yarn.registry.webapp;

import com.google.protobuf.ByteString;

import org.apache.hadoop.yarn.registry.RegistryException;
import org.apache.hadoop.yarn.registry.Storage;
import org.apache.hadoop.yarn.registry.StorageException;
import org.apache.hadoop.yarn.registry.Storage.UpdateFunc;
import org.apache.hadoop.yarn.registry.Encryptor;
import org.apache.hadoop.yarn.registry.VersionedKeyEncryptor;
import org.apache.hadoop.yarn.registry.security.SecurityData;
import org.apache.hadoop.yarn.registry.security.Owner;
import org.apache.hadoop.yarn.registry.Registry;
import org.apache.hadoop.yarn.registry.proto.RegistryProtos.VirtualHostProto;
import org.apache.hadoop.yarn.registry.proto.RegistryProtos.ServerProto;
import org.apache.hadoop.yarn.registry.proto.RegistryProtos.SecurityDataProto;
import org.apache.hadoop.yarn.registry.security.SelfSignedSSL;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.ArrayNode;

import java.util.List;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.Collection;
import java.util.ArrayList;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import javax.ws.rs.Path;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.DELETE;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@Path("/v1")
public class WebApp {
  private static final Log LOG = LogFactory.getLog(WebApp.class);
  private Storage storage = null;
  private Encryptor encrypt = null;

  /**
   * Set the servlet context
   * @param context the contex the servlet is operating in.
   */ 
  @Context
  public void setContext(ServletContext context) {
    LOG.warn("Setting context");
    setStorage((Storage)context.getAttribute(Registry.STORAGE_ATTRIBUTE));
    setEncryptor((Encryptor)context.getAttribute(Registry.ENCRYPTOR_ATTRIBUTE));
  }

  public void setStorage(Storage storage) {
    if (storage == null) {
      throw new IllegalArgumentException("Storage cannot be set to null");
    }
    this.storage = storage;
  }

  public void setEncryptor(Encryptor encrypt) {
    if (encrypt == null) {
      throw new IllegalArgumentException("Encryptor cannot be set to null");
    }
    this.encrypt = encrypt;
  }

  @GET
  @Path("/status")
  @Produces(MediaType.TEXT_PLAIN)
  public String status() {
    LOG.warn("/status GET");
    return "OK";
  }

  public static void assertValidHostName(String hostName) throws RegistryException {
    Storage.validateVHKey(hostName);
  }

  public static VirtualHostProto readVH(JsonNode node, Encryptor encrypt) throws Exception {
    String hostName = Utils.getString(node, false, "virtualHost", "name");
    String scheme = Utils.getString(node, true, "virtualHost", "scheme");
    Integer port = Utils.getInt(node, true, "virtualHost", "port");
    Integer timeout = Utils.getInt(node, true, "virtualHost", "serverTimeoutSecs");
    List<String> owner = Utils.getStringList(node, true, "virtualHost", "owner");
    assertValidHostName(hostName);
    VirtualHostProto.Builder b = VirtualHostProto.newBuilder().setName(hostName);
    if (scheme != null) {
      b.setScheme(scheme);
    }
    if (port != null) {
      b.setPort(port);
    }
    if (timeout == null) {
      timeout = 10 * 60; //10 mins default timeout
    }
    b.setServerTimeoutSecs(timeout);
    if (owner != null && !owner.isEmpty()) {
      b.addAllOwner(owner);
    }
    ObjectNode secure = Utils.getObjectNode(node, true, "virtualHost", "securityData");
    if (secure != null) {
      Iterator<Map.Entry<String,JsonNode>> it = secure.getFields();
      while (it.hasNext()) {
        Map.Entry<String, JsonNode> entry = it.next();
        SecurityData sd = SecurityData.getSDFor(entry.getKey());
        if (sd == null) {
          throw new RegistryException(entry.getKey()+" is not a registered Security Data Provider", 400);
        }
        byte [] ret = null;
        try {
          ret = sd.validateAndPrepareForStorage(entry.getValue());
          if (ret != null) {
            byte [] encRet = encrypt.encrypt(ret);
            b.addSecurityData(SecurityDataProto.newBuilder().setName(entry.getKey()).setData(ByteString.copyFrom(encRet)));
          }
        } finally {
          if (ret != null) {
            for(int i=0; i<ret.length; i++) {
              ret[i] = '0';
            }
          }
        }
      }
    }
    return b.build();
  }

  public static VirtualHostProto readVH(String json, Encryptor encrypt) throws Exception {
    return readVH(Utils.parseJson(json), encrypt);
  }

  public static String toJson(VirtualHostProto proto, Encryptor encrypt, boolean isPublic) throws Exception {
    JsonNode rootNode = Utils.createObjectNode();
    ObjectNode vh = (ObjectNode)rootNode.with("virtualHost");
    vh.put("name", proto.getName());
    if (proto.hasScheme()) {
      vh.put("scheme", proto.getScheme());
    }
    if (proto.hasPort()) {
      vh.put("port", proto.getPort());
    }
    vh.put("serverTimeoutSecs", proto.getServerTimeoutSecs());
    if (proto.getOwnerCount() > 0) {
      ArrayNode arr = vh.putArray("owner");
      for (String owner: proto.getOwnerList()) {
        arr.add(owner);
      }
    }
    if (proto.getSecurityDataCount() > 0) {
      ObjectNode security = vh.putObject("securityData");
      for (SecurityDataProto sdp: proto.getSecurityDataList()) {
        SecurityData sd = SecurityData.getSDFor(sdp.getName());
        if (sd != null) {
          JsonNode json;
          byte [] data = {};
          try {
            data = encrypt.decrypt(sdp.getData().toByteArray());
            if (isPublic) {
              json = sd.getPublicInformation(data);
            } else {
              json = sd.getAllInformation(data);
            }
          } finally {
            for (int i=0; i<data.length; i++) {
              data[i] = '0';
            }
          }
          if (json != null) {
            security.put(sdp.getName(), json);
          }
        }
      }
    }
    return Utils.toString(rootNode);
  }

  public static ServerProto readServer(JsonNode node, String serverId) throws Exception {
    String jsonId = Utils.getString(node, true, "server", "serverId");
    String host = Utils.getString(node, false, "server", "host");
    Integer port = Utils.getInt(node, true, "server", "port");
    long now = System.currentTimeMillis();
    if (jsonId != null && !jsonId.equals(serverId)) {
      throw new IllegalArgumentException("Two different server ids were given. "+jsonId+" and "+serverId);
    }
    assertValidHostName(host);
    ServerProto.Builder b = ServerProto.newBuilder()
        .setServerId(serverId)
        .setHost(host)
        .setHbTimestamp(now);
    if (port != null) {
      b.setPort(port);
    }
    return b.build();
  }

  public static ServerProto readServer(String json, String serverId) throws Exception {
    return readServer(Utils.parseJson(json), serverId);
  }

  public static void populateFields(ObjectNode server, ServerProto proto) {
    server.put("serverId", proto.getServerId());
    server.put("host", proto.getHost());
    server.put("hb", proto.getHbTimestamp());
    if (proto.hasPort()) {
      server.put("port", proto.getPort());
    }
  }

  public static String toJson(ServerProto proto) throws Exception {
    JsonNode rootNode = Utils.createObjectNode();
    populateFields((ObjectNode)rootNode.with("server"), proto);
    return Utils.toString(rootNode);
  }

  public static String toJson(List<ServerProto> protoList) throws Exception {
    ObjectNode rootNode = Utils.createObjectNode();
    ArrayNode arr = rootNode.putArray("server");
    for (ServerProto proto : protoList) {
      populateFields(arr.addObject(), proto);
    }
    return Utils.toString(rootNode);
  }

  public static String toJson(String rootName, Collection<String> values) throws Exception {
    ObjectNode rootNode = Utils.createObjectNode();
    ArrayNode arr = rootNode.putArray(rootName);
    for (String val : values) {
      arr.add(val);
    }
    return Utils.toString(rootNode);
  }

  public static String toJson(String rootName, Map<String,String> values) throws Exception {
    ObjectNode rootNode = Utils.createObjectNode();
    ObjectNode data = rootNode.putObject(rootName);
    for (Map.Entry<String,String> val : values.entrySet()) {
      data.put(val.getKey(), val.getValue());
    }
    return Utils.toString(rootNode);
  }

  public static void assertIsOwnerOrAdmin(HttpServletRequest req, VirtualHostProto proto) throws Exception {
    //If there are no SecurityData objects then the VirtualHost is not secure, (Perhaps we want to make this more explicit)
    if (proto.getSecurityDataCount() > 0) {
      Owner.assertIsOwnerOrAdmin(req, proto.getOwnerList());
    }
  }

  private static class Recrypt implements UpdateFunc<byte[]> {
    private VersionedKeyEncryptor vke;

    public Recrypt(VersionedKeyEncryptor vke) {
      this.vke = vke;
    }

    @Override
    public byte[] update(byte [] data) throws StorageException {
      try {
        VirtualHostProto proto = VirtualHostProto.parseFrom(data);
        if (proto.getSecurityDataCount() <= 0) {
          return data;
        }
        VirtualHostProto.Builder b = VirtualHostProto.newBuilder(proto);
        for (int i = 0; i < b.getSecurityDataCount(); i++) {
          SecurityDataProto orig = b.getSecurityData(i);
          byte [] newData = vke.recrypt(orig.getData().toByteArray());
          b.setSecurityData(i, SecurityDataProto.newBuilder().setName(orig.getName()).setData(ByteString.copyFrom(newData)));
        }
        return b.build().toByteArray();
      } catch (Exception e) {
        throw new StorageException("Error re-encrypting virtual host "+e.getMessage(),e);
      }
    }
  };

  @GET
  @Path("/admin/virtualHostRecrypt")
  @Produces({MediaType.APPLICATION_JSON})
  public String getVirtualHostNames(@Context HttpServletRequest req) throws Exception {
    LOG.warn("/virtualHost GET");
    Owner.assertIsAdmin(req);
    if (!(encrypt instanceof VersionedKeyEncryptor)) {
      throw new RegistryException("Encryptor does not support this operation", 412);
    }
    Recrypt recrypt = new Recrypt((VersionedKeyEncryptor)encrypt);
    HashMap<String, String> status = new HashMap<String, String>();
    for (String vh: storage.listVirtualHostKeys()) {
      try {
        storage.updateVirtualHost(vh,recrypt);
        status.put(vh,"OK");
      } catch (Exception e) {
        status.put(vh, "Error: "+e.getMessage());
      }
    }
    return toJson("result", status);
  }



  @GET
  @Path("/virtualHost")
  @Produces({MediaType.APPLICATION_JSON})
  public String getVirtualHostNames() throws Exception {
    LOG.warn("/virtualHost GET");

    return toJson("virtualHostNames", storage.listVirtualHostKeys());
  }


  @POST
  @Path("/virtualHost")
  @Consumes({MediaType.APPLICATION_JSON})
  public void addVirtualHost(String postBody, @Context HttpServletRequest req) throws Exception {
    LOG.warn("/virtualHost POST");

    VirtualHostProto vh = readVH(postBody, encrypt);
    List<String> owners = vh.getOwnerList();
    if ((owners.size() <= 0) && (vh.getSecurityDataCount() > 0)) {
      throw new RegistryException("Cannot create a secure virtual host without a list of owners", 400);
    } else if ((owners.size() != 0) && (vh.getSecurityDataCount() == 0)) {
      throw new RegistryException("Cannot create a non-secure virtual host with a list of owners", 400);
    }

    if (!Owner.isOwnerOrAdmin(req, owners) && vh.getSecurityDataCount() > 0) {
      throw new RegistryException("Attempting to create a secure virtual host without being the owner", 403);
    }
    storage.addVirtualHost(vh.getName(), vh.toByteArray());
  }

  @DELETE
  @Path("/virtualHost/{virtualHost}")
  public void deleteVirtualHost(@PathParam("virtualHost") String hostName, @Context HttpServletRequest req)
    throws Exception {
    LOG.warn("/virtualHost/"+hostName+" DELETE");

    VirtualHostProto proto = VirtualHostProto.parseFrom(storage.getVirtualHost(hostName));
    assertIsOwnerOrAdmin(req, proto);

    storage.deleteServers(hostName);
    storage.deleteVirtualHost(hostName);
  } 

  @GET
  @Path("/virtualHost/{virtualHost}")
  @Produces({MediaType.APPLICATION_JSON})
  public String getVirtualHost(@PathParam("virtualHost") String hostName, @Context HttpServletRequest req)
    throws  Exception {
    LOG.warn("/virtualHost/"+hostName+" GET");
    VirtualHostProto proto = VirtualHostProto.parseFrom(storage.getVirtualHost(hostName));
    return toJson(proto, encrypt, !Owner.isOwnerOrAdmin(req, proto.getOwnerList()));
  }


  @GET
  @Path("/virtualHost/{virtualHost}/SelfSignedSSL.cert")
  @Produces({MediaType.TEXT_PLAIN})
  public String getVirtualHostSelfSignedCert(@PathParam("virtualHost") String hostName) throws  Exception {
    LOG.warn("/virtualHost/"+hostName+"/SelfSignedSSL.cert GET");
    VirtualHostProto proto = VirtualHostProto.parseFrom(storage.getVirtualHost(hostName));
    if (proto.getSecurityDataCount() > 0) {
      for (SecurityDataProto sdp: proto.getSecurityDataList()) {
        if ("SelfSignedSSL".equals(sdp.getName())) {
          return SelfSignedSSL.getCert(encrypt.decrypt(sdp.getData().toByteArray()));
        }
      }
    }
    throw new StorageException("No cert found for "+hostName, 404);
  }

  @GET
  @Path("/virtualHost/{virtualHost}/server")
  @Produces({MediaType.APPLICATION_JSON})
  public String getServerList(@PathParam("virtualHost") String hostName)
    throws Exception {
    LOG.warn("/virtualHost/"+hostName+"/server GET");

    VirtualHostProto proto = VirtualHostProto.parseFrom(storage.getVirtualHost(hostName));
    long mustBeAfter = System.currentTimeMillis() - (proto.getServerTimeoutSecs() * 1000);

    Collection<byte[]> found = storage.listServers(hostName);
    ArrayList<ServerProto> sp = new ArrayList<ServerProto>(found.size());
    for (byte[] data: found) {
      ServerProto server = ServerProto.parseFrom(data);
      if (server.getHbTimestamp() >= mustBeAfter) {
        sp.add(server);
      }
    }
    return toJson(sp);
  }

  @PUT
  @Path("/virtualHost/{virtualHost}/server/{serverId}")
  @Consumes({MediaType.APPLICATION_JSON})
  public void putServer(@PathParam("virtualHost") String hostName, 
                        @PathParam("serverId") String serverId,
                        String putBody, @Context HttpServletRequest req) throws Exception {
    LOG.warn("/virtualHost/"+hostName+"/server/"+serverId+" PUT");

    VirtualHostProto proto = VirtualHostProto.parseFrom(storage.getVirtualHost(hostName));
    assertIsOwnerOrAdmin(req, proto);

    ServerProto server = readServer(putBody, serverId);
    storage.putServer(hostName, serverId, server.toByteArray());
  }

  @GET
  @Path("/virtualHost/{virtualHost}/server/{serverId}")
  @Produces({MediaType.APPLICATION_JSON})
  public String getServer(@PathParam("virtualHost") String hostName, 
                          @PathParam("serverId") String serverId) throws Exception {
    LOG.warn("/virtualHost/"+hostName+"/server/"+serverId+" GET");
    ServerProto proto = ServerProto.parseFrom(storage.getServer(hostName, serverId));
    return toJson(proto);
  }

  @DELETE
  @Path("/virtualHost/{virtualHost}/server/{serverId}")
  public void deleteServer(@PathParam("virtualHost") String hostName, 
                          @PathParam("serverId") String serverId, @Context HttpServletRequest req) throws Exception {
    LOG.warn("/virtualHost/"+hostName+"/server/"+serverId+" DELETE");

    VirtualHostProto proto = VirtualHostProto.parseFrom(storage.getVirtualHost(hostName));
    assertIsOwnerOrAdmin(req, proto);

    storage.deleteServer(hostName, serverId);
  }
}
