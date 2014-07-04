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


import org.apache.hadoop.yarn.registry.RegistryException;

import java.io.StringWriter;
import java.io.IOException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.ext.ExceptionMapper;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.ws.rs.WebApplicationException;

@Provider
public class ErrorMapper implements ExceptionMapper<Throwable> {
  private static final Log LOG = LogFactory.getLog(ErrorMapper.class);

  public static String convertToJSON(Throwable t) {
    try {
      StringWriter ret = new StringWriter();
      JsonFactory jf = new JsonFactory();
      JsonGenerator gen = jf.createJsonGenerator(ret);
      gen.writeStartObject();
      gen.writeFieldName("error");
      gen.writeStartObject();
      String message = t.getMessage();
      if (message == null) {
        message = "No Error Description Given";
      }
      gen.writeStringField("description", message);
      gen.writeEndObject(); //error
      gen.writeEndObject(); //body
      gen.flush(); 
      return ret.toString();
    } catch (IOException e) {
      LOG.error("Could not turn Throwable into JSON", e);
      throw new RuntimeException(e);
    }
  }

  public Response toResponse(Throwable t) {
    if (t instanceof WebApplicationException) {
      return ((WebApplicationException)t).getResponse();
    }

    int errorCode = 500;
    if (t instanceof RegistryException) {
      errorCode = ((RegistryException)t).getStatus();
    } else if (t instanceof IllegalArgumentException) {
      errorCode = 400;
    }

    if (errorCode >= 500) {
      LOG.error("Internal error in web app", t);
    }

    return Response.status(errorCode).
      entity(convertToJSON(t)).
      type(MediaType.APPLICATION_JSON).
      build();
  }
}
