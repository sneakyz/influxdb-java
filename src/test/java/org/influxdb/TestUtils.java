package org.influxdb;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestUtils {

	public static String getInfluxIP() {
		String ip = "10.10.0.5";
		
		Map<String, String> getenv = System.getenv();
		if (getenv.containsKey("INFLUXDB_IP")) {
			ip = getenv.get("INFLUXDB_IP");
		}
		
		return ip;
	}

  public static void main(String[] args) throws Exception {
    InfluxDB influxDB = InfluxDBFactory.connect("http://" + TestUtils.getInfluxIP() + ":8086", "root", "root");
    influxDB.enableBatch(100, 200, TimeUnit.MILLISECONDS);

  }

}
