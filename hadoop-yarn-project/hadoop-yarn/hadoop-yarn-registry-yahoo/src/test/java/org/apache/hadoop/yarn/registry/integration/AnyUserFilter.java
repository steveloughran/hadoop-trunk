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

import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.security.Principal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AnyUserFilter extends FilterInitializer {
  private static final Log LOG = LogFactory.getLog(AnyUserFilter.class);
  private static final String FILTER_NAME = "TEST_ANY_USER_FILTER";
  private static final String FILTER_CLASS = FilterImpl.class.getName();
  public static final String USER_HEADER = "USER_NAME";

  public static class AnyUserPrincipal implements Principal {
    private final String name;
  
    public AnyUserPrincipal(String name) {
      this.name = name;
    }
  
    @Override
    public String getName() {
      return name;
    }
  }
 
  public static class AnyUserServletRequestWrapper extends HttpServletRequestWrapper {
    private final AnyUserPrincipal principal;

    public AnyUserServletRequestWrapper(HttpServletRequest request, 
        AnyUserPrincipal principal) {
      super(request);
      this.principal = principal;
    }

    @Override
    public Principal getUserPrincipal() {
      return principal;
    }

    @Override
    public String getRemoteUser() {
      return principal.getName();
    }

    @Override
    public boolean isUserInRole(String role) {
      //No role info so far
      return false;
    }
  }
 
  public static class FilterImpl implements Filter {
    @Override
    public void init(FilterConfig conf) throws ServletException {
      //Empty
    }
  
    @Override
    public void destroy() {
      //Empty
    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse resp,
        FilterChain chain) throws IOException, ServletException {
      if(!(req instanceof HttpServletRequest)) {
        throw new ServletException("This filter only works for HTTP/HTTPS");
      }
    
      HttpServletRequest httpReq = (HttpServletRequest)req;
    
      String user = httpReq.getHeader(USER_HEADER);
    
      if (user == null) {
        LOG.warn("Could not find " + USER_HEADER + " header, so user will not be set");
        chain.doFilter(req, resp);
      } else {
        LOG.warn("Setting user for this request to "+ user);
        final AnyUserPrincipal principal = new AnyUserPrincipal(user);
        ServletRequest requestWrapper = new AnyUserServletRequestWrapper(httpReq, 
            principal);
        chain.doFilter(requestWrapper, resp);
      }
    }
  }

  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    Map<String, String> params = new HashMap<String, String>();
    container.addFilter(FILTER_NAME, FILTER_CLASS, params);
  }
}
