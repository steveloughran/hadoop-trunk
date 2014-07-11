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

package org.apache.hadoop.yarn.registry.client.api.selectors;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Select an entry from a map of entries
 * @param <T> type of entries to select
 */
public abstract class Selector<T> implements Iterable<Map.Entry<String, T>> {

  protected final Map<String, T> entries;

  protected Selector(Map<String, T> entries) {
    this.entries = new HashMap<String, T>(entries);
  }


  /**
   * Iterator with no entries

   */
  protected class NoEntryIterator implements Iterator<Map.Entry<String, T>> {
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public Map.Entry<String, T> next() {
      throw new NoSuchElementException();
    }

    @Override
    public void remove() {

    }
  }


  /**
   * Iterator with no entries
   */
  protected class SingleEntryIterator implements Iterator<Map.Entry<String, T>> {

    private Map.Entry<String, T> entry;

    public SingleEntryIterator(Map.Entry<String, T> entry) {
      this.entry = entry;
    }

    @Override
    public boolean hasNext() {
      return entry != null;
    }

    @Override
    public Map.Entry<String, T> next() {
      Map.Entry<String, T> result = entry;
      entry = null;
      if (result == null) {
        throw new NoSuchElementException();
      }
      return result;
    }

    @Override
    public void remove() {

    }
  }


  protected static class SelectorEntry<T> implements Map.Entry<String, T> {

    private final String key;
    private T value;

    public SelectorEntry(String key, T value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public String getKey() {
      return key;
    }

    @Override
    public T getValue() {
      return value;
    }

    @Override
    public T setValue(T v) {
      return value = v;
    }
  }
}
