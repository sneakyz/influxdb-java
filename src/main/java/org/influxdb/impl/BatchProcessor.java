package org.influxdb.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A BatchProcessor can be attached to a InfluxDB Instance to collect single point writes and
 * aggregates them to BatchPoints to get a better write performance.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
public class BatchProcessor {

	private static final Logger logger = Logger.getLogger(BatchProcessor.class.getName());
	protected final BlockingQueue<BatchEntry> queue = new LinkedBlockingQueue<>(300000);
	private final ExecutorService executor = Executors.newFixedThreadPool(1);
	final InfluxDBImpl influxDB;
	final int actions;
	private final TimeUnit flushIntervalUnit;
	private final int flushInterval;

	/**
	 * The Builder to create a BatchProcessor instance.
	 */
	public static final class Builder {
		private final InfluxDBImpl influxDB;
		private int actions;
		private TimeUnit flushIntervalUnit;
		private int flushInterval;

		/**
		 * @param influxDB
		 *            is mandatory.
		 */
		public Builder(final InfluxDB influxDB) {
			this.influxDB = (InfluxDBImpl) influxDB;
		}

		/**
		 * The number of actions after which a batchwrite must be performed.
		 *
		 * @param maxActions
		 *            number of Points written after which a write must happen.
		 * @return this Builder to use it fluent
		 */
		public Builder actions(final int maxActions) {
			this.actions = maxActions;
			return this;
		}

		/**
		 * The interval at which at least should issued a write.
		 *
		 * @param interval
		 *            the interval
		 * @param unit
		 *            the TimeUnit of the interval
		 *
		 * @return this Builder to use it fluent
		 */
		public Builder interval(final int interval, final TimeUnit unit) {
			this.flushInterval = interval;
			this.flushIntervalUnit = unit;
			return this;
		}

		/**
		 * Create the BatchProcessor.
		 *
		 * @return the BatchProcessor instance.
		 */
		public BatchProcessor build() {
			Preconditions.checkNotNull(this.actions, "actions may not be null");
			Preconditions.checkNotNull(this.flushInterval, "flushInterval may not be null");
			Preconditions.checkNotNull(this.flushIntervalUnit, "flushIntervalUnit may not be null");
			return new BatchProcessor(this.influxDB, this.actions, this.flushIntervalUnit, this.flushInterval);
		}
	}

	static class BatchEntry {
		private final Point point;
		private final String db;
		private final String rp;

		public BatchEntry(final Point point, final String db, final String rp) {
			super();
			this.point = point;
			this.db = db;
			this.rp = rp;
		}

		public Point getPoint() {
			return this.point;
		}

		public String getDb() {
			return this.db;
		}

		public String getRp() {
			return this.rp;
		}
	}

	/**
	 * Static method to create the Builder for this BatchProcessor.
	 *
	 * @param influxDB
	 *            the influxdb database handle.
	 * @return the Builder to create the BatchProcessor.
	 */
	public static Builder builder(final InfluxDB influxDB) {
		return new Builder(influxDB);
	}

	BatchProcessor(final InfluxDBImpl influxDB, final int actions, final TimeUnit flushIntervalUnit,
			final int flushInterval) {
		super();
		this.influxDB = influxDB;
		this.actions = actions;
		this.flushIntervalUnit = flushIntervalUnit;
		this.flushInterval = flushInterval;

    writeAsync();
	}

  // Flush at specified Rate
  private void writeAsync() {
    this.executor.submit(() -> {
      long ts = System.currentTimeMillis();
      while (true) {
        try {
          int timeRemaing = flushInterval;
          long start = System.currentTimeMillis();

          Map<String, BatchPoints> databaseToBatchPoints = Maps.newHashMap();
          List<BatchEntry> batchEntries = new ArrayList<>(this.actions);
          if (queue.size() > 100000) {
            // Interval 5 seconds
            if (start - ts > 5000) {
              logger.warning("buffer point size: " + queue.size());
              ts = start;
            }
          }

          for (int i = 0; i < actions; i++) {
            long wait = timeRemaing - (System.currentTimeMillis() - start);
            if (wait <= 0) {
              break;
            }
            BatchEntry entry = queue.poll(wait, TimeUnit.MILLISECONDS);
            if (entry != null) {
              batchEntries.add(entry);
            }
          }

          for (BatchEntry batchEntry : batchEntries) {
            String dbName = batchEntry.getDb();
            if (!databaseToBatchPoints.containsKey(dbName)) {
              BatchPoints batchPoints = BatchPoints.database(dbName).retentionPolicy(batchEntry.getRp()).build();
              databaseToBatchPoints.put(dbName, batchPoints);
            }
            Point point = batchEntry.getPoint();
            databaseToBatchPoints.get(dbName).point(point);
          }
          long t1 = System.currentTimeMillis();
          for (BatchPoints batchPoints : databaseToBatchPoints.values()) {
            BatchProcessor.this.influxDB.write(batchPoints);
          }
          long t2 = System.currentTimeMillis() - t1;
          if (t2 > 500) {
            logger.warning("write " + batchEntries.size() + " points, " + t2 + " mills");
          }
        } catch (Throwable t) {
          logger.log(Level.SEVERE, "Batch could not be sent. Data is lost", t);
        }
      }
    });
  }

	void write() {
		try {
			if (this.queue.isEmpty()) {
				return;
			}

			Map<String, BatchPoints> databaseToBatchPoints = Maps.newHashMap();
			List<BatchEntry> batchEntries = new ArrayList<>(this.actions);
      if (queue.size() > 100000) {
        logger.warning("buffer point size: " + queue.size());
      }
      for (int i = 0; i < actions; i++) {
        BatchEntry entry = queue.poll();
        if (entry == null) {
          break;
        } else {
          batchEntries.add(entry);
        }
      }

			for (BatchEntry batchEntry : batchEntries) {
				String dbName = batchEntry.getDb();
				if (!databaseToBatchPoints.containsKey(dbName)) {
					BatchPoints batchPoints = BatchPoints.database(dbName).retentionPolicy(batchEntry.getRp()).build();
					databaseToBatchPoints.put(dbName, batchPoints);
				}
				Point point = batchEntry.getPoint();
				databaseToBatchPoints.get(dbName).point(point);
			}

			for (BatchPoints batchPoints : databaseToBatchPoints.values()) {
				BatchProcessor.this.influxDB.write(batchPoints);
			}
		} catch (Throwable t) {
			// any exception would stop the scheduler
			logger.log(Level.SEVERE, "Batch could not be sent. Data will be lost", t);
		}
	}

	/**
	 * Put a single BatchEntry to the cache for later processing.
	 *
	 * @param batchEntry
	 *            the batchEntry to write to the cache.
	 */
	void put(final BatchEntry batchEntry) {
		this.queue.offer(batchEntry);
		if (this.queue.size() >= this.actions) {
			writeAsync();
		}
	}

	/**
	 * Flush the current open writes to influxdb and end stop the reaper thread. This should only be
	 * called if no batch processing is needed anymore.
	 *
	 */
	void flush() {
		this.write();
		this.executor.shutdown();
	}

}
