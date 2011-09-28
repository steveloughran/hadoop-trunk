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
package org.apache.hadoop.net;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.NoRouteToHostException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.net.ConnectException;
import java.nio.channels.SocketChannel;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.*;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.util.ReflectionUtils;

@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class NetUtils {
  private static final Log LOG = LogFactory.getLog(NetUtils.class);
  
  private static Map<String, String> hostToResolved = 
          new HashMap<String, String>();
  private static final String FOR_MORE_DETAILS_SEE
      = " For more details see:  ";
  public static final String UNKNOWN_HOST = "(unknown)";
  public static final String HADOOP_WIKI = "http://wiki.apache.org/hadoop/";

  /**
   * Get the socket factory for the given class according to its
   * configuration parameter
   * <tt>hadoop.rpc.socket.factory.class.&lt;ClassName&gt;</tt>. When no
   * such parameter exists then fall back on the default socket factory as
   * configured by <tt>hadoop.rpc.socket.factory.class.default</tt>. If
   * this default socket factory is not configured, then fall back on the JVM
   * default socket factory.
   * 
   * @param conf the configuration
   * @param clazz the class (usually a {@link VersionedProtocol})
   * @return a socket factory
   */
  public static SocketFactory getSocketFactory(Configuration conf,
      Class<?> clazz) {

    SocketFactory factory = null;

    String propValue =
        conf.get("hadoop.rpc.socket.factory.class." + clazz.getSimpleName());
    if ((propValue != null) && (propValue.length() > 0))
      factory = getSocketFactoryFromProperty(conf, propValue);

    if (factory == null)
      factory = getDefaultSocketFactory(conf);

    return factory;
  }

  /**
   * Get the default socket factory as specified by the configuration
   * parameter <tt>hadoop.rpc.socket.factory.default</tt>
   * 
   * @param conf the configuration
   * @return the default socket factory as specified in the configuration or
   *         the JVM default socket factory if the configuration does not
   *         contain a default socket factory property.
   */
  public static SocketFactory getDefaultSocketFactory(Configuration conf) {

    String propValue = conf.get("hadoop.rpc.socket.factory.class.default");
    if ((propValue == null) || (propValue.length() == 0))
      return SocketFactory.getDefault();

    return getSocketFactoryFromProperty(conf, propValue);
  }

  /**
   * Get the socket factory corresponding to the given proxy URI. If the
   * given proxy URI corresponds to an absence of configuration parameter,
   * returns null. If the URI is malformed raises an exception.
   * 
   * @param propValue the property which is the class name of the
   *        SocketFactory to instantiate; assumed non null and non empty.
   * @return a socket factory as defined in the property value.
   */
  public static SocketFactory getSocketFactoryFromProperty(
      Configuration conf, String propValue) {

    try {
      Class<?> theClass = conf.getClassByName(propValue);
      return (SocketFactory) ReflectionUtils.newInstance(theClass, conf);

    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException("Socket Factory class not found: " + cnfe);
    }
  }

  /**
   * Util method to build socket addr from either:
   *   <host>:<post>
   *   <fs>://<host>:<port>/<path>
   */
  public static InetSocketAddress createSocketAddr(String target) {
    return createSocketAddr(target, -1);
  }

  /**
   * Util method to build socket addr from either:
   *   <host>
   *   <host>:<post>
   *   <fs>://<host>:<port>/<path>
   */
  public static InetSocketAddress createSocketAddr(String target,
                                                   int defaultPort) {
    if (target == null) {
      throw new IllegalArgumentException("Target address cannot be null.");
    }
    int colonIndex = target.indexOf(':');
    if (colonIndex < 0 && defaultPort == -1) {
      throw new RuntimeException("Not a host:port pair: " + target);
    }
    String hostname;
    int port = -1;
    if (!target.contains("/")) {
      if (colonIndex == -1) {
        hostname = target;
      } else {
        // must be the old style <host>:<port>
        hostname = target.substring(0, colonIndex);
        port = Integer.parseInt(target.substring(colonIndex + 1));
      }
    } else {
      // a new uri
      URI addr = new Path(target).toUri();
      hostname = addr.getHost();
      port = addr.getPort();
    }

    if (port == -1) {
      port = defaultPort;
    }
  
    if (getStaticResolution(hostname) != null) {
      hostname = getStaticResolution(hostname);
    }
    return new InetSocketAddress(hostname, port);
  }

  /**
   * Adds a static resolution for host. This can be used for setting up
   * hostnames with names that are fake to point to a well known host. For e.g.
   * in some testcases we require to have daemons with different hostnames
   * running on the same machine. In order to create connections to these
   * daemons, one can set up mappings from those hostnames to "localhost".
   * {@link NetUtils#getStaticResolution(String)} can be used to query for
   * the actual hostname. 
   * @param host
   * @param resolvedName
   */
  public static void addStaticResolution(String host, String resolvedName) {
    synchronized (hostToResolved) {
      hostToResolved.put(host, resolvedName);
    }
  }
  
  /**
   * Retrieves the resolved name for the passed host. The resolved name must
   * have been set earlier using 
   * {@link NetUtils#addStaticResolution(String, String)}
   * @param host
   * @return the resolution
   */
  public static String getStaticResolution(String host) {
    synchronized (hostToResolved) {
      return hostToResolved.get(host);
    }
  }
  
  /**
   * This is used to get all the resolutions that were added using
   * {@link NetUtils#addStaticResolution(String, String)}. The return
   * value is a List each element of which contains an array of String 
   * of the form String[0]=hostname, String[1]=resolved-hostname
   * @return the list of resolutions
   */
  public static List <String[]> getAllStaticResolutions() {
    synchronized (hostToResolved) {
      Set <Entry <String, String>>entries = hostToResolved.entrySet();
      if (entries.size() == 0) {
        return null;
      }
      List <String[]> l = new ArrayList<String[]>(entries.size());
      for (Entry<String, String> e : entries) {
        l.add(new String[] {e.getKey(), e.getValue()});
      }
    return l;
    }
  }
  
  /**
   * Returns InetSocketAddress that a client can use to 
   * connect to the server. Server.getListenerAddress() is not correct when
   * the server binds to "0.0.0.0". This returns "127.0.0.1:port" when
   * the getListenerAddress() returns "0.0.0.0:port".
   * 
   * @param server
   * @return socket address that a client can use to connect to the server.
   */
  public static InetSocketAddress getConnectAddress(Server server) {
    InetSocketAddress addr = server.getListenerAddress();
    if (addr.getAddress().getHostAddress().equals("0.0.0.0")) {
      addr = new InetSocketAddress("127.0.0.1", addr.getPort());
    }
    return addr;
  }
  
  /**
   * Same as getInputStream(socket, socket.getSoTimeout()).<br><br>
   * 
   * From documentation for {@link #getInputStream(Socket, long)}:<br>
   * Returns InputStream for the socket. If the socket has an associated
   * SocketChannel then it returns a 
   * {@link SocketInputStream} with the given timeout. If the socket does not
   * have a channel, {@link Socket#getInputStream()} is returned. In the later
   * case, the timeout argument is ignored and the timeout set with 
   * {@link Socket#setSoTimeout(int)} applies for reads.<br><br>
   *
   * Any socket created using socket factories returned by {@link NetUtils},
   * must use this interface instead of {@link Socket#getInputStream()}.
   *     
   * @see #getInputStream(Socket, long)
   * 
   * @param socket
   * @return InputStream for reading from the socket.
   * @throws IOException
   */
  public static InputStream getInputStream(Socket socket) 
                                           throws IOException {
    return getInputStream(socket, socket.getSoTimeout());
  }
  
  /**
   * Returns InputStream for the socket. If the socket has an associated
   * SocketChannel then it returns a 
   * {@link SocketInputStream} with the given timeout. If the socket does not
   * have a channel, {@link Socket#getInputStream()} is returned. In the later
   * case, the timeout argument is ignored and the timeout set with 
   * {@link Socket#setSoTimeout(int)} applies for reads.<br><br>
   * 
   * Any socket created using socket factories returned by {@link NetUtils},
   * must use this interface instead of {@link Socket#getInputStream()}.
   *     
   * @see Socket#getChannel()
   * 
   * @param socket
   * @param timeout timeout in milliseconds. This may not always apply. zero
   *        for waiting as long as necessary.
   * @return InputStream for reading from the socket.
   * @throws IOException
   */
  public static InputStream getInputStream(Socket socket, long timeout) 
                                           throws IOException {
    return (socket.getChannel() == null) ? 
          socket.getInputStream() : new SocketInputStream(socket, timeout);
  }
  
  /**
   * Same as getOutputStream(socket, 0). Timeout of zero implies write will
   * wait until data is available.<br><br>
   * 
   * From documentation for {@link #getOutputStream(Socket, long)} : <br>
   * Returns OutputStream for the socket. If the socket has an associated
   * SocketChannel then it returns a 
   * {@link SocketOutputStream} with the given timeout. If the socket does not
   * have a channel, {@link Socket#getOutputStream()} is returned. In the later
   * case, the timeout argument is ignored and the write will wait until 
   * data is available.<br><br>
   * 
   * Any socket created using socket factories returned by {@link NetUtils},
   * must use this interface instead of {@link Socket#getOutputStream()}.
   * 
   * @see #getOutputStream(Socket, long)
   * 
   * @param socket
   * @return OutputStream for writing to the socket.
   * @throws IOException
   */  
  public static OutputStream getOutputStream(Socket socket) 
                                             throws IOException {
    return getOutputStream(socket, 0);
  }
  
  /**
   * Returns OutputStream for the socket. If the socket has an associated
   * SocketChannel then it returns a 
   * {@link SocketOutputStream} with the given timeout. If the socket does not
   * have a channel, {@link Socket#getOutputStream()} is returned. In the later
   * case, the timeout argument is ignored and the write will wait until 
   * data is available.<br><br>
   * 
   * Any socket created using socket factories returned by {@link NetUtils},
   * must use this interface instead of {@link Socket#getOutputStream()}.
   * 
   * @see Socket#getChannel()
   * 
   * @param socket
   * @param timeout timeout in milliseconds. This may not always apply. zero
   *        for waiting as long as necessary.
   * @return OutputStream for writing to the socket.
   * @throws IOException   
   */
  public static OutputStream getOutputStream(Socket socket, long timeout) 
                                             throws IOException {
    return (socket.getChannel() == null) ? 
            socket.getOutputStream() : new SocketOutputStream(socket, timeout);            
  }
  
  /**
   * This is a drop-in replacement for 
   * {@link Socket#connect(SocketAddress, int)}.
   * In the case of normal sockets that don't have associated channels, this 
   * just invokes <code>socket.connect(endpoint, timeout)</code>. If 
   * <code>socket.getChannel()</code> returns a non-null channel,
   * connect is implemented using Hadoop's selectors. This is done mainly
   * to avoid Sun's connect implementation from creating thread-local 
   * selectors, since Hadoop does not have control on when these are closed
   * and could end up taking all the available file descriptors.
   * 
   * @see java.net.Socket#connect(java.net.SocketAddress, int)
   * 
   * @param socket
   * @param endpoint 
   * @param timeout - timeout in milliseconds
   */
  public static void connect(Socket socket, 
                             SocketAddress endpoint, 
                             int timeout) throws IOException {
    if (socket == null || endpoint == null || timeout < 0) {
      throw new IllegalArgumentException("Illegal argument for connect()");
    }
    
    SocketChannel ch = socket.getChannel();
    
    if (ch == null) {
      // let the default implementation handle it.
      socket.connect(endpoint, timeout);
    } else {
      SocketIOWithTimeout.connect(ch, endpoint, timeout);
    }

    // There is a very rare case allowed by the TCP specification, such that
    // if we are trying to connect to an endpoint on the local machine,
    // and we end up choosing an ephemeral port equal to the destination port,
    // we will actually end up getting connected to ourself (ie any data we
    // send just comes right back). This is only possible if the target
    // daemon is down, so we'll treat it like connection refused.
    if (socket.getLocalPort() == socket.getPort() &&
        socket.getLocalAddress().equals(socket.getInetAddress())) {
      LOG.info("Detected a loopback TCP socket, disconnecting it");
      socket.close();
      throw new ConnectException(
        "Localhost targeted connection resulted in a loopback. " +
        "No daemon is listening on the target port.");
    }
  }
  
  /** 
   * Given a string representation of a host, return its ip address
   * in textual presentation.
   * 
   * @param name a string representation of a host:
   *             either a textual representation its IP address or its host name
   * @return its IP address in the string format
   */
  public static String normalizeHostName(String name) {
    if (Character.digit(name.charAt(0), 10) != -1) { // it is an IP
      return name;
    } else {
      try {
        InetAddress ipAddress = InetAddress.getByName(name);
        return ipAddress.getHostAddress();
      } catch (UnknownHostException e) {
        return name;
      }
    }
  }
  
  /** 
   * Given a collection of string representation of hosts, return a list of
   * corresponding IP addresses in the textual representation.
   * 
   * @param names a collection of string representations of hosts
   * @return a list of corresponding IP addresses in the string format
   * @see #normalizeHostName(String)
   */
  public static List<String> normalizeHostNames(Collection<String> names) {
    List<String> hostNames = new ArrayList<String>(names.size());
    for (String name : names) {
      hostNames.add(normalizeHostName(name));
    }
    return hostNames;
  }

  /**
   * Performs a sanity check on the list of hostnames/IPs to verify they at least
   * appear to be valid.
   * @param names - List of hostnames/IPs
   * @throws UnknownHostException
   */
  public static void verifyHostnames(String[] names) throws UnknownHostException {
    for (String name: names) {
      if (name == null) {
        throw new UnknownHostException("null hostname found");
      }
      // The first check supports URL formats (e.g. hdfs://, etc.). 
      // java.net.URI requires a schema, so we add a dummy one if it doesn't
      // have one already.
      URI uri = null;
      try {
        uri = new URI(name);
        if (uri.getHost() == null) {
          uri = new URI("http://" + name);
        }
      } catch (URISyntaxException e) {
        uri = null;
      }
      if (uri == null || uri.getHost() == null) {
        throw new UnknownHostException(name + " is not a valid Inet address");
      }
    }
  }

  private static final Pattern ipPattern = // Pattern for matching hostname to ip:port
    Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}:?\\d*");
  
  /**
   * Attempt to obtain the host name of a name specified by ip address.  
   * Check that the node name is an ip addr and if so, attempt to determine
   * its host name.  If the name is not an IP addr, or the actual name cannot
   * be determined, return null.
   * 
   * @return Host name or null
   */
  public static String getHostNameOfIP(String ip) {
    // If name is not an ip addr, don't bother looking it up
    if(!ipPattern.matcher(ip).matches())
      return null;
    
    String hostname = "";
    try {
      String n = ip.substring(0, ip.indexOf(':'));
      hostname = InetAddress.getByName(n).getHostName();
    } catch (UnknownHostException e) {
      return null;
    }
    
    return hostname; 
  }

  /**
   * Return hostname without throwing exception.
   * @return hostname
   */
  public static String getHostname() {
    try {return "" + InetAddress.getLocalHost();}
    catch(UnknownHostException uhe) {return "" + uhe;}
  }
  
  /**
   * Checks if {@code host} is a local host name and return {@link InetAddress}
   * corresponding to that address.
   * 
   * @param host the specified host
   * @return a valid local {@link InetAddress} or null
   * @throws SocketException if an I/O error occurs
   */
  public static InetAddress getLocalInetAddress(String host)
      throws SocketException {
    if (host == null) {
      return null;
    }
    InetAddress addr = null;
    try {
      addr = InetAddress.getByName(host);
      if (NetworkInterface.getByInetAddress(addr) == null) {
        addr = null; // Not a local address
      }
    } catch (UnknownHostException ignore) { }
    return addr;
  }

  /**
   * Take an IOException , the local host port and remote host port details and
   * return an IOException with the input exception as the cause and also
   * include the host details. The new exception provides the stack trace of the
   * place where the exception is thrown and some extra diagnostics information.
   * If the exception is BindException or ConnectException or
   * UnknownHostException or SocketTimeoutException, return a new one of the
   * same type; Otherwise return an IOException.
   *
   * @param destHost target host (nullable)
   * @param destPort target port
   * @param localHost local host (nullable)
   * @param localPort local port
   * @param exception the caught exception.
   * @return an exception to throw
   */
  public static IOException wrapException(final String destHost,
                                          final int destPort,
                                          final String localHost,
                                          final int localPort,
                                          final IOException exception) {
    if (exception instanceof BindException) {
      return new BindException(
          "Problem binding to ["
              + localHost
              + ":"
              + localPort
              + "] "
              + exception
              + ";"
              + see("BindException"));
    } else if (exception instanceof ConnectException) {
      // connection refused; include the host:port in the error
      return (ConnectException) new ConnectException(
          "Call From "
              + localHost
              + " to "
              + destHost
              + ":"
              + destPort
              + " failed on connection exception: "
              + exception
              + ";"
              + see("ConnectionRefused"))
          .initCause(exception);
    } else if (exception instanceof UnknownHostException) {
      return (UnknownHostException) new UnknownHostException(
            "Invalid host name: "
                + getHostDetailsAsString(destHost, destPort, localHost)
                + exception
                + ";"
                + see("UnknownHost"))
            .initCause(exception);
    } else if (exception instanceof SocketTimeoutException) {
      return (SocketTimeoutException) new SocketTimeoutException(
              "Call From "
                + localHost + " to " + destHost + ":" + destPort
                + " failed on socket timeout exception: " + exception
                + ";"
                + see("SocketTimeout"))
                .initCause(exception);
    } else if (exception instanceof NoRouteToHostException) {
      return (NoRouteToHostException) new NoRouteToHostException(
          "No Route to Host from  "
                 + localHost + " to " + destHost + ":" + destPort
                 + " failed on socket timeout exception: " + exception
                 + ";"
                 + see("NoRouteToHost"))
          .initCause(exception);
    }
    else {
      return (IOException) new IOException("Failed on local exception: "
          + exception
          + "; Host Details : "
          + getHostDetailsAsString(destHost, destPort, localHost))
          .initCause(exception);

    }
  }

  private static String see(final String entry) {
    return FOR_MORE_DETAILS_SEE + HADOOP_WIKI + entry;
  }

  /**
   * Get the host details as a string
   * @param destHost destinatioon host (nullable)
   * @param destPort destination port
   * @param localHost local host (nullable)
   * @return a string describing the destination host:port and the local host
   */
  private static String getHostDetailsAsString(final String destHost, 
      final int destPort,
      final String localHost) {
    StringBuilder hostDetails = new StringBuilder(27);
    hostDetails.append("local host is: ")
        .append(quoteHost(localHost))
        .append("; ");
    hostDetails.append("destination host is: \"").append(quoteHost(destHost))
        .append(":")
        .append(destPort).append("; ");
    return hostDetails.toString();
  }

  /**
   * Quote a hostname if it is not null
   * @param hostname the hostname; nullable
   * @return a quoted hostname or {@link #UNKNOWN_HOST} if the hostname is null
   */
  private static String quoteHost(final String hostname) {
    return (hostname != null) ?
        ("\"" + hostname + "\"")
        : UNKNOWN_HOST;
  }

  /**
   * Given an InetAddress, checks to see if the address is a local address, by
   * comparing the address with all the interfaces on the node.
   * @param addr address to check if it is local node's address
   * @return true if the address corresponds to the local node
   */
  public static boolean isLocalAddress(InetAddress addr) {
    // Check if the address is any local or loop back
    boolean local = addr.isAnyLocalAddress() || addr.isLoopbackAddress();

    // Check if the address is defined on any interface
    if (!local) {
      try {
        local = NetworkInterface.getByInetAddress(addr) != null;
      } catch (SocketException e) {
        local = false;
      }
    }
    return local;
  }
}
