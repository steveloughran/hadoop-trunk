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
import org.apache.log4j.spi.ThrowableInformation;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

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
  private static final JsonFactory factory = new MappingJsonFactory();
  public static final String TIME = "time";
  public static final String LEVEL = "level";
  public static final String NAME = "name";
  public static final String THREAD = "thread";
  public static final String MESSAGE = "message";
  public static final String STACK = "stack";
  public static final String JSON_TYPE = "application/json";

  public Log4Json() {
  }


  /**
   * 
   * @return the mime type of JSON
   */
  @Override
  public String getContentType() {
    return JSON_TYPE;
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
   * @return the writer
   * @throws IOException on problems generating the JSON
   */
  public Writer toJson(final Writer writer, final LoggingEvent event) 
      throws IOException {
    ThrowableInformation ti = event.getThrowableInformation();
    toJson(writer, 
           event.getLoggerName(),
           event.getTimeStamp(),
           event.getLevel().toString(), 
           event.getThreadName(),
           event.getRenderedMessage(), 
           ti);
    return writer;
  }

  /**
   * Build a JSON entry from the parameters. This is public for testing.
   * @param writer destination
   * @param loggerName logger name
   * @param timeStamp time_t value
   * @param level level string
   * @param threadName name of the thread
   * @param message rendered message
   * @param ti nullable thrown information
   * @return the writer
   * @throws IOException on any problem
   */
  public Writer toJson(final Writer writer,
                       final String loggerName,
                       final long timeStamp,
                       final String level,
                       final String threadName,
                       final String message,
                       final ThrowableInformation ti) throws IOException {
    JsonGenerator json = factory.createJsonGenerator(writer);
    json.writeStartObject();
    json.writeStringField(NAME, loggerName);
    json.writeNumberField(TIME, timeStamp);
    json.writeStringField(LEVEL, level);
    json.writeStringField(THREAD, threadName);
    json.writeStringField(MESSAGE, message);
    if (ti != null) {
      String[] stackTrace = ti.getThrowableStrRep();
      json.writeArrayFieldStart(STACK);
      for (String row : stackTrace) {
        json.writeString(row);
      }
      json.writeEndArray();
    }
    json.writeEndObject();
    json.flush();
    json.close();
    return writer;
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

  /**
   * For use in tests
   *
   * @param json incoming JSON to parse
   * @return a node tree
   * @throws IOException on any parsing problems
   */
  public static JsonNode parse(String json) throws IOException {
    ObjectMapper mapper = new ObjectMapper(factory);
    return mapper.readTree(json);
  }
}
