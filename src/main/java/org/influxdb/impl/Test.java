package org.influxdb.impl;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author sneaky
 * @since 1.0.0
 */
public class Test {
  static ExecutorService executorService = Executors.newFixedThreadPool(1);
  static InfluxDB influxDB = InfluxDBFactory.connect("http://10.10.0.5:8086", "root", "root");
  public static void main(String[] args) throws Exception {
    System.out.println(new Date(1472014780000L));
    System.out.println(new Date(1472014783799L));
  }
}
