/*
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

package org.apache.hadoop.yarn.registry.client.exceptions;

import org.apache.hadoop.security.AccessControlException;
import org.apache.http.HttpStatus;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;

public class ExceptionGenerator {

  @SuppressWarnings("ThrowableInstanceNeverThrown")
  public static IOException generate(int statusCode,
      String uri,
      String message,
      Exception e) {
    IOException result;
    switch (statusCode) {
      case HttpStatus.SC_NOT_FOUND:
        result = new FileNotFoundException(message);
        break;
      case HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE:
        result = new EOFException(message);

        break;
      case HttpStatus.SC_FORBIDDEN: //forbidden
      case HttpStatus.SC_UNAUTHORIZED: //unauth
        result = new AccessControlException(message);
        break;
      default:
        if (e instanceof HttpErrorProvider) {
          statusCode = ((HttpErrorProvider) e).getStatusCode();
          uri = ((HttpErrorProvider) e).getURI();
        }
        result = new RESTIOException(statusCode, uri, message, e);
    }
    return result;
  }
}
