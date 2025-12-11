package cis5550.generic;

import static cis5550.webserver.Server.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class Coordinator {
	private static final long EXPIRATION_MS = 15000;
	protected String coordinatorHost;
	protected int coordinatorPort;

	protected static class WorkerInfo {
		final String id;
		final String ip;
		final int port;
		final long lastTime;

		WorkerInfo(String id, String ip, int port, long lastTime) {
			this.id = id;
			this.ip = ip;
			this.port = port;
			this.lastTime = lastTime;
		}
		String address() {
			return ip + ":" + port;
		}
	}

	protected final ConcurrentMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();

	public String[] getWorkers() {
		List<WorkerInfo> liveWorkers = getLiveWorkersSorted();

		List<String> addresses = new ArrayList<>();
		for (WorkerInfo w : liveWorkers) {
			addresses.add(w.ip + ":" + w.port);
		}

		return addresses.toArray(new String[0]);
	}

	public String workerTable() {
		long now = System.currentTimeMillis();
		List<WorkerInfo> liveWorkers = getLiveWorkersSorted();

		String rows = "";
		for (WorkerInfo worker : liveWorkers) {
			rows = rows 
					+ "<tr><td style=\"text-align:left\">" 
					+ worker.id + " - " 
					+ "<a href=\"http://" + worker.ip + ":" + worker.port + "/\" target=\"_blank\">" 
					+ worker.ip + ":" + worker.port 
					+ "</a> - Last: " + (now - worker.lastTime) + " ms ago"
					+ "</td></tr>";
		}
		return "<table border=\"1\"><tr><th style=\"text-align:left\">Workers</th></tr>" + rows + "</table>";
	}

	public void registerRoutes() {
		get("/ping", (req, res) -> {
			String id = req.queryParams("id");
			String port = req.queryParams("port");
			if (id == null || id.isEmpty() || port == null) {
				res.status(400, "Bad Request");
				return "Missing 'id' or 'port'";
			}
			int portNum;
			try {
				portNum = Integer.parseInt(port);
			} catch (NumberFormatException e) {
				res.status(400, "Bad Request");
				return "Invalid 'port'";
			}

			String ip = req.ip();
			workers.put(id, new WorkerInfo(id, ip, portNum, System.currentTimeMillis()));
			return "OK";
		});

		get("/workers", (req, res) -> {
			res.type("text/plain");
			List<WorkerInfo> liveWorkers = getLiveWorkersSorted();
			String result = "" + liveWorkers.size() + "\n";

			for (WorkerInfo worker : liveWorkers) {
				result += worker.id + "," + worker.ip + ":" + worker.port + "\n";
			}
			return result;
		});
	}

	protected void startPingThread() {
	}

	private List<WorkerInfo> getLiveWorkersSorted() {
		long curTime = System.currentTimeMillis();

		List<WorkerInfo> liveWorkers = new ArrayList<>();
		for (WorkerInfo worker : workers.values()) {
			if (curTime - worker.lastTime <= EXPIRATION_MS) {
				liveWorkers.add(worker);
			}
		}
		liveWorkers.sort(new Comparator<WorkerInfo>() {
			@Override
			public int compare(WorkerInfo w1, WorkerInfo w2) {
				return w1.id.compareTo(w2.id);
			}
		});

		return liveWorkers;
	}

}
