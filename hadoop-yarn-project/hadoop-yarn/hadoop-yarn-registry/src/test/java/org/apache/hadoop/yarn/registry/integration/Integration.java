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

import java.util.HashSet;
import java.net.ServerSocket;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.registry.Registry;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.methods.RequestEntity;

import org.skyscreamer.jsonassert.JSONAssert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Integration {
  private static final Log LOG = LogFactory.getLog(Integration.class);

  private Registry server = null;
  private int port;
  protected HttpClient client;
  protected String urlBase;

  public synchronized void startServer(YarnConfiguration conf) throws Exception {
    if (server != null) {
      stopServer();
    }
    if (conf == null) {
      conf = new YarnConfiguration();
    }
    ServerSocket s = new ServerSocket(0);
    port = s.getLocalPort();
    s.close();
    urlBase = "http://localhost:"+port+"/registry/v1/";
    conf.set(Registry.REGISTRY_URI_CONF, urlBase);
    HashSet<String> inits = new HashSet<String>(conf.getStringCollection("hadoop.http.filter.initializers"));
    inits.add(AnyUserFilter.class.getName());
    conf.setStrings("hadoop.http.filter.initializers", inits.toArray(new String[inits.size()]));
    server = new Registry();
    server.init(conf);
    server.start();
    client = new HttpClient();
  }

  public synchronized void stopServer() throws Exception {
    if (server != null) {
      server.stop();
      server.join();
      server = null;
    }
  }

  public static String toString(HttpMethod method) throws Exception {
    return toString(method, null);
  }

  public static String toString(HttpMethod method, String msg) throws Exception {
    StringBuffer b = new StringBuffer();
    if (msg != null) {
      b.append(msg)
       .append(" ");
    }
    b.append(method.getURI())
     .append(" - ")
     .append(method.getName())
     .append("\nREQUEST:\nHEADERS:\n");
     for (Header h: method.getRequestHeaders()) {
       b.append(h);
     }
     if (method instanceof EntityEnclosingMethod) {
       RequestEntity entity = ((EntityEnclosingMethod)method).getRequestEntity();
       if (entity instanceof StringRequestEntity) {
         b.append("BODY:\n")
          .append(((StringRequestEntity)entity).getContent())
          .append("\n");
       }
     }
     b.append("\nRESPONSE:\nHEADERS:\n");
     for (Header h: method.getResponseHeaders()) {
       b.append(h);
     }
     String body = method.getResponseBodyAsString();
     if (body != null) {
       b.append("BODY:\n").append(body).append("\n");
     }
     return b.toString();
  }


  public static void assertJsonEquals(String message, String expected, String found, boolean strict) throws Exception {
    try {
      JSONAssert.assertEquals(expected, found, strict);
    } catch (AssertionError e) {
      throw new AssertionError(message + ": expected body\n"+expected+"\n", e);
    }
  }

  public String get(String path, int code, String user) throws Exception {
    String url = urlBase + path;
    GetMethod get = new GetMethod(url);
    if (user != null) {
      get.addRequestHeader(new Header(AnyUserFilter.USER_HEADER, user));
    }
    try {
      client.executeMethod(get);
      assertEquals(toString(get, "incorrect status code"), code, get.getStatusCode());
      LOG.info(toString(get));
      return get.getResponseBodyAsString();
    } finally {
      get.releaseConnection();
    }
  }

  public void assertGet(String path, int code) throws Exception {
    assertJsonGet(path, null, code);
  }

  public void assertJsonGet(String path, String jsonBody) throws Exception {
    assertJsonGet(path, jsonBody, 200, true);
  }

  public void assertJsonGet(String path, String jsonBody, boolean strict) throws Exception {
    assertJsonGet(path, jsonBody, 200, strict);
  }

  public void assertJsonGet(String path, String jsonBody, int code) throws Exception {
    assertJsonGet(path, jsonBody, code, true);
  }


  public void assertJsonGet(String path, String jsonBody, int code, boolean strict) throws Exception {
    assertJsonGet(path, jsonBody, code, strict, null);
  }

  public void assertJsonGet(String path, String jsonBody, boolean strict, String user) throws Exception {
    assertJsonGet(path, jsonBody, 200, strict, user);
  }

  public void assertJsonGet(String path, String jsonBody, int code, boolean strict, String user) throws Exception {
    String url = urlBase + path;
    GetMethod get = new GetMethod(url);
    if (user != null) {
      get.addRequestHeader(new Header(AnyUserFilter.USER_HEADER, user));
    }
    try {
      client.executeMethod(get);
      assertEquals(toString(get, "incorrect status code"), code, get.getStatusCode());
      if (jsonBody != null) {
        assertJsonEquals(toString(get, "Body does not match"), jsonBody, get.getResponseBodyAsString(), strict);
      }
      LOG.info(toString(get));
    } finally {
      get.releaseConnection();
    }
  }

  public void assertJsonPost(String path, String postJson) throws Exception {
    assertJsonPost(path, postJson, null, 204);
  }
  
  public void assertJsonPost(String path, String postJson, String respJson, int code) throws Exception {
    assertJsonPost(path, postJson, respJson, code, null);
  }

  public void assertJsonPost(String path, String postJson, String user) throws Exception {
    assertJsonPost(path, postJson, null, 204, user);
  }

  public void assertJsonPost(String path, String postJson, int code, String user) throws Exception {
    assertJsonPost(path, postJson, null, code, user);
  }

  public void assertJsonPost(String path, String postJson, String respJson, int code, String user) throws Exception {
    String url = urlBase + path;
    PostMethod post = new PostMethod(url);
    if (user != null) {
      post.addRequestHeader(new Header(AnyUserFilter.USER_HEADER, user));
    }
    post.setRequestEntity(new StringRequestEntity(postJson, "application/json", "utf-8"));
    try {
      client.executeMethod(post);
      assertEquals(toString(post, "incorrect status code"), code, post.getStatusCode());
      if (respJson != null) {
        assertJsonEquals(toString(post, "Body does not match"), respJson, post.getResponseBodyAsString(), true);
      }
      LOG.info(toString(post));
    } finally {
      post.releaseConnection();
    }
  }

  public void assertJsonPut(String path, String putJson) throws Exception {
    assertJsonPut(path, putJson, null, 204, null);
  }

  public void assertJsonPut(String path, String putJson, String user) throws Exception {
    assertJsonPut(path, putJson, null, 204, user);
  }

  public void assertJsonPut(String path, String putJson, String respJson, int code) throws Exception {
    assertJsonPut(path, putJson, respJson, code, null);
  }

  public void assertJsonPut(String path, String putJson, int code, String user) throws Exception {
    assertJsonPut(path, putJson, null, code, user);
  }

  public void assertJsonPut(String path, String putJson, String respJson, int code, String user) throws Exception {
    String url = urlBase + path;
    PutMethod put = new PutMethod(url);
    if (user != null) {
      put.addRequestHeader(new Header(AnyUserFilter.USER_HEADER, user));
    }
    put.setRequestEntity(new StringRequestEntity(putJson, "application/json", "utf-8"));
    try {
      client.executeMethod(put);
      assertEquals(toString(put, "incorrect status code"), code, put.getStatusCode());
      if (respJson != null) {
        assertJsonEquals(toString(put, "Body does not match"), respJson, put.getResponseBodyAsString(), true);
      }
      LOG.info(toString(put));
    } finally {
      put.releaseConnection();
    }
  }
}

