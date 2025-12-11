package cis5550.generic;

import java.util.Random;
import cis5550.tools.HTTP;

public class Worker {
	protected String coordinatorAddr;
	protected int httpPort;
	protected String workerId;

	private static final long PING_INTERVAL_MS = 5000;

	protected void startPingThread() {
		Thread t = new Thread(() -> {
			for (;;) {
				try {
					Thread.sleep(PING_INTERVAL_MS);

					String urlStr = "http://" + coordinatorAddr + "/ping?id=" + workerId + "&port=" + httpPort;
					HTTP.doRequest("GET", urlStr, null);

				} catch (Exception e) {

				}
			}
		});
		t.start();
	}

	protected static String randomId5() {
		Random r = new Random();
		StringBuilder sb = new StringBuilder(5);
		for (int i = 0; i < 5; i++) {
			sb.append((char) ('a' + r.nextInt(26)));
		}
		return sb.toString();
	}
}
