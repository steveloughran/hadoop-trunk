package org.apache.hadoop.fs.swift.exceptions;

/**
 * Exception raised when an attempt is made to use a closed stream
 */
public class SwiftConnectionClosedException extends SwiftException {

  public static final String MESSAGE =
    "Connection to Swift service has been closed";

  public SwiftConnectionClosedException() {
    super(MESSAGE);
  }

  public SwiftConnectionClosedException(String reason) {
    super(MESSAGE + ": " + reason);
  }

}
