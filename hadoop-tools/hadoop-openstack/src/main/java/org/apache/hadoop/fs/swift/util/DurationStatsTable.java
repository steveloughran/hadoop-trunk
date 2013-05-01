package org.apache.hadoop.fs.swift.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Build a duration stats table to which you can add statistics.
 * Designed to be multithreaded
 */
public class DurationStatsTable {

  private Map<String,DurationStats> statsTable
    = new HashMap<String, DurationStats>(6);

  /**
   * Add an operation
   * @param operation operation name
   * @param duration duration
   */
  public void add(String operation, Duration duration, boolean success) {
    DurationStats durationStats;
    String key = operation;
    if (!success) {
      key += "-FAIL";
    }
    synchronized (this) {
      durationStats = statsTable.get(key);
      if (durationStats == null) {
        durationStats = new DurationStats(key);
        statsTable.put(key, durationStats);
      }
    }
    synchronized (durationStats) {
      durationStats.add(duration);
    }
  }

  /**
   * Get the current duration statistics
   * @return a snapshot of the statistics
   */
   public synchronized List<DurationStats> getDurationStatistics() {
     List<DurationStats> results = new ArrayList<DurationStats>(statsTable.size());
     for (DurationStats stat: statsTable.values()) {
       results.add(new DurationStats(stat));
     }
     return results;
   }

  /**
   * reset the values of the statistics. This doesn't delete them, merely zeroes them.
   */
  public synchronized void reset() {
    for (DurationStats stat : statsTable.values()) {
      stat.reset();
    }
  }
}
