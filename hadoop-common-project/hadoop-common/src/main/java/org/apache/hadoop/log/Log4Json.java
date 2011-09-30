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


package org.apache.hadoop.log;

import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

/**
 * This offers a log layout for JSON, with whatever test entry points aid
 * testing.
 *
 * Some features. <ol> <li>Time is published as a time_t event since 1/1/1970
 * -this is the fastest to generate.</li> </ol>
 */
public class Log4Json extends Layout {
  public static final String FIELD_THREAD = "";

  /**
   * Jackson factories are thread safe when constructing parsers and generators.
   * They are not thread safe in configure methods; if there is to be any
   * configuration it must be done in a static intializer block.
   */
  private static final JsonFactory factory = new JsonFactory();
  protected String name;
  protected String stack;

  public Log4Json() {
  }

  @Override
  public String format(LoggingEvent event) {
    try {
      return toJson(event);
    } catch (IOException e) {
      //this really should not happen, and rather than throw an exception
      //which may hide the real problem, the log class is printed
      //in JSON format. The classname is used to ensure valid JSON is 
      //returned without playing escaping games
      return "{ \"logfailure\":\"" + e.getClass().toString() + "\"}";
    }
  }

  /**
   * Convert an event to JSON
   *
   * @param event the event -must not be null
   * @return a string value
   * @throws IOException on problems generating the JSON
   */
  public String toJson(LoggingEvent event) throws IOException {
    StringWriter writer = new StringWriter();
    toJson(writer, event);
    return writer.toString();
  }

  /**
   * Convert an event to JSON
   *
   * @param writer the destination writer
   * @param event the event -must not be null
   * @throws IOException on problems generating the JSON
   */
  public void toJson(Writer writer, LoggingEvent event) throws IOException {
    JsonGenerator json = factory.createJsonGenerator(writer);
    json.writeStartObject();
    json.writeStringField("name", event.getLoggerName());
    json.writeNumberField("time", event.getTimeStamp());
    json.writeStringField("level", event.getLevel().toString());
    json.writeStringField("thread", event.getThreadName());
    json.writeStringField("message", event.getRenderedMessage());
    String[] stackTrace = event.getThrowableStrRep();
    if (stackTrace != null) {
      json.writeArrayFieldStart("stack");
      for (String row : stackTrace) {
        json.writeString(row);
      }
      json.writeEndArray();
    }
    json.writeEndObject();
  }

  @Override
  public boolean ignoresThrowable() {
    return false;
  }

  /**
   * Do nothing
   */
  @Override
  public void activateOptions() {
  }
}
