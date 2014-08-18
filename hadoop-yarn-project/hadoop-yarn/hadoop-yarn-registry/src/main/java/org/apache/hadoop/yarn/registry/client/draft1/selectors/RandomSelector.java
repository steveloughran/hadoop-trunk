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

package org.apache.hadoop.yarn.registry.client.draft1.selectors;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Select an entry at random. This may return the same entry on two
 * successive calls.
 * @param <T>
 */
public class RandomSelector<T> extends Selector<T> {

  private final Random random;

  public RandomSelector(Map<String, T> entries) {
    super(entries);
    random = new Random();
  }


  private Map.Entry<String, T> select() {
    Map<String, T> map = entries;
    int index = random.nextInt(map.size());
    Set<Map.Entry<String, T>> entrySet = map.entrySet();
    for (Map.Entry<String, T> entry : entrySet) {
      if (index == 0) {
        return entry;
      }
      index--;
    }
    return null;
  }

  @Override
  public Iterator<Map.Entry<String, T>> iterator() {
    return new RandomEntry();
  }

  protected class RandomEntry implements Iterator<Map.Entry<String, T>> {
    @Override
    public boolean hasNext() {
      return !entries.isEmpty();
    }

    @Override
    public Map.Entry<String, T> next() {
      return select();
    }

    @Override
    public void remove() {

    }
  }
}
