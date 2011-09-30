/**
 *
 */

package org.apache.hadoop.log;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Appender;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.spi.HierarchyEventListener;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.log4j.spi.LoggerRepository;
import org.apache.log4j.spi.NOPLoggerRepository;
import org.apache.log4j.spi.ThrowableInformation;
import org.junit.Test;

import java.io.StringWriter;
import java.io.Writer;
import java.net.NoRouteToHostException;
import java.util.Enumeration;
import java.util.Vector;

public class TestLog4Json extends TestCase {

  private static Log LOG = LogFactory.getLog(TestLog4Json.class);

  @Test
  public void testConstruction() throws Throwable {
    Log4Json l4j = new Log4Json();
    String outcome = l4j.toJson(new StringWriter(),
                                "name", 0, "DEBUG", "thread1",
                                "hello, world", null).toString();
    println("testConstruction", outcome);
  }

  @Test
  public void testException() throws Throwable {
    Exception e =
        new NoRouteToHostException("that box caught fire 3 years ago");
    ThrowableInformation ti = new ThrowableInformation(e);
    Log4Json l4j = new Log4Json();
    String outcome = l4j.toJson(new StringWriter(),
                                "testException",
                                -4365664,
                                "INFO",
                                "quoted\"",
                                "new line\n and {}",
                                ti)
        .toString();
    println("testException", outcome);
  }

  @Test
  public void testLog() throws Throwable {
    String message = "test message";
    Throwable throwable = null;
    String json = logOut(message, throwable);
    println("testLog", json);
  }

  /**
   * Print out what's going on. The logging APIs aren't used and the text
   * delimited for more details
   *
   * @param name name of operation
   * @param text text to print 
   */
  private void println(String name, String text) {
    System.out.println(name + ": #" + text + "#");
  }

  private String logOut(String message, Throwable throwable) {
    StringWriter writer = new StringWriter();
    Logger logger = createLogger(writer);
    logger.info(message, throwable);
    //remove and close the appender
    logger.removeAllAppenders();
    return writer.toString();
  }

  public Logger createLogger(Writer writer) {
    TestLoggerRepository repo = new TestLoggerRepository();
    Logger logger = repo.getLogger("test");
    Log4Json layout = new Log4Json();
    WriterAppender appender = new WriterAppender(layout, writer);
    logger.addAppender(appender);
    return logger;
  }

  /**
   * This test logger avoids integrating with the main runtimes Logger hierarchy
   * in ways the reader does not want to know.
   */
  private static class TestLogger extends Logger {
    private TestLogger(String name, LoggerRepository repo) {
      super(name);
      repository = repo;
      setLevel(Level.INFO);
    }

  }
  
  public static class TestLoggerRepository implements LoggerRepository {
    @Override
    public void addHierarchyEventListener(HierarchyEventListener listener) {
    }

    @Override
    public boolean isDisabled(int level) {
      return false;
    }

    @Override
    public void setThreshold(Level level) {
    }

    @Override
    public void setThreshold(String val) {
    }

    @Override
    public void emitNoAppenderWarning(Category cat) {
    }

    @Override
    public Level getThreshold() {
      return Level.ALL;
    }

    @Override
    public Logger getLogger(String name) {
      return new TestLogger(name, this);
    }

    @Override
    public Logger getLogger(String name, LoggerFactory factory) {
      return new TestLogger(name, this);
    }

    @Override
    public Logger getRootLogger() {
      return new TestLogger("root", this);
    }

    @Override
    public Logger exists(String name) {
      return null;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public Enumeration getCurrentLoggers() {
      return new Vector().elements();
    }

    @Override
    public Enumeration getCurrentCategories() {
      return new Vector().elements();
    }

    @Override
    public void fireAddAppenderEvent(Category logger, Appender appender) {
    }

    @Override
    public void resetConfiguration() {
    }
  }
}
