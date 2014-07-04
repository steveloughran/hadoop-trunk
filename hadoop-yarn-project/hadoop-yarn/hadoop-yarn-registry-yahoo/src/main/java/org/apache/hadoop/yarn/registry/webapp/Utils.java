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

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Utils {
  private static final Log LOG = LogFactory.getLog(Utils.class);
  private static final ThreadLocal<ObjectMapper> mapper = 
    new ThreadLocal <ObjectMapper> () {
      @Override
      protected ObjectMapper initialValue() {
        return new ObjectMapper();
      }
    };
    
  public static JsonNode parseJson(String text) throws IOException {
    return mapper.get().readTree(text);
  }

  public static String toString(JsonNode node) throws IOException{
    return mapper.get().writeValueAsString(node);
  }

  public static ObjectNode createObjectNode() {
    return mapper.get().createObjectNode();
  }
 
  public static ObjectMapper getMapper() {
    return mapper.get();
  }

  public static ObjectNode mkNode() {
      return mapper.get().createObjectNode();
  }

  public static byte[] toBytes(JsonNode node) throws IOException {
      return mapper.get().writeValueAsBytes(node);
  }

  public static JsonNode getNode(JsonNode node, boolean optional, String ... paths) {
    JsonNode ret = node;
    for (String path: paths) {
      ret = ret.path(path);
    }
    if (!optional && ret.isMissingNode()) {
      throw new IllegalArgumentException("Could not find required path "+Arrays.toString(paths));
    }
    return ret;
  }

  public static String getString(JsonNode node, boolean optional, String ... paths) {
    JsonNode found = getNode(node, optional, paths);
    if (optional && found.isMissingNode()) {
      return null;
    }
    if (!found.isTextual()) {
      throw new IllegalArgumentException(Arrays.toString(paths)+" is expected to be a string");
    }
    return found.getTextValue();
  }

  public static ObjectNode getObjectNode(JsonNode node, boolean optional, String ... paths) {
    JsonNode found = getNode(node, optional, paths);
    if (optional && found.isMissingNode()) {
      return null;
    }
    if (!found.isObject()) {
      throw new IllegalArgumentException(Arrays.toString(paths)+" is expected to be a json object");
    }
    return (ObjectNode)found;
  }


  public static Integer getInt(JsonNode node, boolean optional, String ... paths) {
    JsonNode found = getNode(node, optional, paths);
    if (optional && found.isMissingNode()) {
      return null;
    }
    if (!found.isNumber() || JsonParser.NumberType.INT != found.getNumberType()) {
      throw new IllegalArgumentException(Arrays.toString(paths)+" is expected to be an integer");
    }
    return found.getValueAsInt();
  }

  public static List<String> getStringList(JsonNode node, boolean optional, String ... paths) {
    JsonNode found = getNode(node, optional, paths);
    if (optional && found.isMissingNode()) {
      return null;
    }
    if (!found.isArray()) {
      throw new IllegalArgumentException(Arrays.toString(paths)+" is expected to be an array");
    }
    ArrayNode an = (ArrayNode)found;
    ArrayList<String> ret = new ArrayList<String>(an.size());
    for (int i = 0; i < an.size(); i++) {
      JsonNode n = an.get(i);
      if (n == null || !n.isTextual()) {
        throw new IllegalArgumentException(Arrays.toString(paths)+"["+i+"] is expected to be a string");
      }
      ret.add(n.getTextValue());
    }
    return ret;
  }
}
