package org.apache.hadoop.fs.swift.http;

public class HttpBodyContent {
  private final long contentLength;
  private final HttpInputStreamWithRelease inputStream;

  /**
   * build a body response
   * @param inputStream input stream from the operatin
   * @param contentLength length of content; may be -1 for "don't know"
   */
  public HttpBodyContent(HttpInputStreamWithRelease inputStream,
                         long contentLength) {
    this.contentLength = contentLength;
    this.inputStream = inputStream;
  }

  public long getContentLength() {
    return contentLength;
  }

  public HttpInputStreamWithRelease getInputStream() {
    return inputStream;
  }
}
