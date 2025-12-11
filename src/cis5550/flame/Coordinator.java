package cis5550.flame;

import java.util.*;
import java.net.*;
import java.io.*;
import java.lang.reflect.*;

import static cis5550.webserver.Server.*;
import cis5550.kvs.KVSClient;
import cis5550.tools.*;

class Coordinator extends cis5550.generic.Coordinator {

	private static final String version = "v1.5";
	static int nextJobID = 1;
	public static KVSClient kvs;
	public static String kvsCoord;
	private static volatile int concurrency = 20;
	private static Coordinator instance;

	// cache KVS worker list
	private static volatile List<String[]> cachedKVSWorkers = null;
	private static volatile long lastKVSWorkerFetch = 0;
	private static final long KVS_WORKER_CACHE_TTL_MS = 30000;
	private static final int MAX_KVS_FETCH_RETRIES = 3;
	private static final int KVS_FETCH_RETRY_DELAY_MS = 1000;

	public static void main(String args[]) {
		instance = new Coordinator();
		if (args.length != 2) {
			System.err.println("Syntax: Coordinator <port> <kvsCoordinator>");
			System.exit(1);
		}

		// read concurrency from system property
		String concurrencyProp = System.getProperty("flame.coordinator.concurrency");
		if (concurrencyProp != null && !concurrencyProp.isEmpty()) {
			try {
				int newConcurrency = Integer.parseInt(concurrencyProp);
				if (newConcurrency >= 1) {
					concurrency = newConcurrency;
					System.out.println("[Flame Coordinator] Concurrency set to: " + concurrency);
				}
			} catch (NumberFormatException e) {
				System.err
						.println("Warning: Invalid flame.coordinator.concurrency value, using default " + concurrency);
			}
		}

		// set port and kvs coordinator
		int myPort = Integer.valueOf(args[0]);
		kvs = new KVSClient(args[1]);
		kvsCoord = args[1];
		port(myPort);

		instance.registerRoutes();

		// get request to return worker table
		get("/", (request, response) -> {
			response.type("text/html");
			return "<html>" + "<head>" + "<title>Flame coordinator</title>" + "</head>" + "<body>"
					+ "<h3>Flame Coordinator</h3>\n" + instance.workerTable() + "</body>" + "</html>";
		});

		// post request to submit job
		post("/submit", (request, response) -> {

			// get class name
			String className = request.queryParams("class");
			if (className == null) {
				response.status(400, "Bad request");
				return "Missing class name (parameter 'class')";
			}

			// get arguments
			Vector<String> argVector = new Vector<String>();
			for (int i = 1; request.queryParams("arg" + i) != null; i++)
				argVector.add(URLDecoder.decode(request.queryParams("arg" + i), "UTF-8"));

			// get workers
			String[] workers = instance.getWorkers();
			Thread threads[] = new Thread[workers.length];
			String results[] = new String[workers.length];

			// process jar bytes
			final byte[] jarBytes = request.bodyAsBytes();
			for (int i = 0; i < workers.length; i++) {
				final String url = "http://" + workers[i] + "/useJAR";
				final int j = i;
				threads[i] = new Thread("JAR upload #" + (i + 1)) {
					public void run() {
						try {
							HTTP.Response r = HTTP.doRequestWithTimeout("POST", url, jarBytes, 10000, false);
							results[j] = new String(r.body());
						} catch (Exception e) {
							results[j] = "Exception: " + e;
						}
					}
				};
				threads[i].start();
			}

			for (int i = 0; i < threads.length; i++) {
				try {
					threads[i].join();
				} catch (InterruptedException ie) {
				}
			}

			// create job ID and jar file
			int id = nextJobID++;
			File jarsDir = new File("jar");
			jarsDir.mkdirs();
			String jarName = "job-" + id + ".jar";
			File jarFile = new File(jarsDir, jarName);
			FileOutputStream fos = new FileOutputStream(jarFile);
			fos.write(request.bodyAsBytes());
			fos.close();

			// invoke run method
			try {
				FlameContextImpl ctx = new FlameContextImpl(kvs);
				Loader.invokeRunMethod(jarFile, className, ctx, argVector);
				return ctx.getOutputOrDefault();
			} catch (IllegalAccessException iae) {
				response.status(400, "Bad request");
				return "Double-check that the class " + className
						+ " contains a public static run(FlameContext, String[]) method, and that the class itself is public!";
			} catch (NoSuchMethodException iae) {
				response.status(400, "Bad request");
				return "Double-check that the class " + className
						+ " contains a public static run(FlameContext, String[]) method";
			} catch (InvocationTargetException ite) {
				StringWriter sw = new StringWriter();
				ite.getCause().printStackTrace(new PrintWriter(sw));
				response.status(500, "Job threw an exception");
				return sw.toString();
			}
		});

		// get request to return version
		get("/version", (request, response) -> {
			return version;
		});
	}

	public static String startRDD(String opPath, String inputTable, byte[] lambdaBytes) throws Exception {
		return startRDD(opPath, inputTable, lambdaBytes, null);
	}

	public static String startRDD(String opPath, String inputTable, byte[] lambdaBytes, String extraQuery)
			throws Exception {
		String outputTable = "pt-flame-" + UUID.randomUUID();
		Partitioner p = new Partitioner();
		List<String[]> kvsWorkers = fetchKVSWorkers();

		if (kvsWorkers.isEmpty())
			throw new RuntimeException("No KVS workers");

		// get KVS workers with IDs
		List<String[]> withIds = new ArrayList<>();
		for (String[] w : kvsWorkers) {
			String addr = w[0];
			String rawId = w[1];
			String ringId;

			if (rawId != null && !rawId.isEmpty()) {
				ringId = rawId;
			} else {
				ringId = Hasher.hash(addr);
			}
			withIds.add(new String[] { addr, ringId });
		}

		// sort KVS workers by ID
		withIds.sort(Comparator.comparing(a -> a[1]));
		int n = withIds.size();
		int numFlameWorkers = instance.getWorkers().length;
		int rangesPerWorker = concurrency;

		// set number of ranges per worker
		if (numFlameWorkers > 1) {
			rangesPerWorker = Math.max(rangesPerWorker, 2);
		}
		p.setKeyRangesPerWorker(rangesPerWorker);

		// add KVS workers to partitioner
		if (n == 1) {
			// special case for single KVS worker
			String addr = withIds.get(0)[0];
			final String MID = "8000000000000000000000000000000000000000";
			p.addKVSWorker(addr, null, MID);
			p.addKVSWorker(addr, MID, null);
		} else {
			for (int i = 0; i < n; i++) {
				String addr = withIds.get(i)[0];
				String thisId = withIds.get(i)[1];
				String prevId = withIds.get((i - 1 + n) % n)[1];
				p.addKVSWorker(addr, prevId, thisId);
			}
		}

		// get Flame workers
		String[] flameWorkers = instance.getWorkers();
		if (flameWorkers == null || flameWorkers.length == 0) {
			throw new RuntimeException("No Flame workers available.");
		}
		// add Flame workers to partitioner
		for (String fw : flameWorkers) {
			p.addFlameWorker(fw);
		}
		// assign partitions
		Vector<Partitioner.Partition> parts = p.assignPartitions();

		if (parts == null || parts.isEmpty())
			throw new RuntimeException("Partitioning failed");

		// create threads for each partition
		for (int i = 0; i < parts.size(); i++) {
			Partitioner.Partition part = parts.get(i);
		}
		Thread[] threads = new Thread[parts.size()];
		RuntimeException[] errs = new RuntimeException[parts.size()];

		// process each partition
		for (int i = 0; i < parts.size(); i++) {
			final int idx = i;
			threads[i] = new Thread(() -> {
				try {
					Partitioner.Partition part = parts.get(idx);
					String u = "http://" + part.assignedFlameWorker + opPath + "?input="
							+ URLEncoder.encode(inputTable, "UTF-8") + "&output="
							+ URLEncoder.encode(outputTable, "UTF-8") + "&kvs=" + URLEncoder.encode(kvsCoord, "UTF-8");
					if (part.fromKey != null) {
						u += "&start=" + URLEncoder.encode(part.fromKey, "UTF-8");
					}
					if (part.toKeyExclusive != null) {
						u += "&end=" + URLEncoder.encode(part.toKeyExclusive, "UTF-8");
					}
					if (extraQuery != null && !extraQuery.isEmpty()) {
						u += "&" + extraQuery;
					}
					HTTP.Response r = HTTP.doRequestWithTimeout("POST", u, lambdaBytes, 2400000, false);

					if (r.statusCode() != 200)
						throw new RuntimeException("Worker " + part.assignedFlameWorker + " HTTP " + r.statusCode());
				} catch (Exception e) {
					errs[idx] = new RuntimeException(e);
				}
			}, "invoke-" + i);
			threads[i].start();
		}

		for (Thread t : threads)
			try {
				t.join();
			} catch (InterruptedException ignored) {
			}
		for (RuntimeException e : errs) {
			if (e != null)
				throw e;
		}
		return outputTable;

	}

	// fetch KVS workers with caching and retry logic
	private static List<String[]> fetchKVSWorkers() throws Exception {
		// check cache
		long now = System.currentTimeMillis();
		if (cachedKVSWorkers != null && (now - lastKVSWorkerFetch) < KVS_WORKER_CACHE_TTL_MS) {
			return cachedKVSWorkers;
		}

		// retry
		Exception lastException = null;
		for (int attempt = 0; attempt < MAX_KVS_FETCH_RETRIES; attempt++) {
			try {
				ArrayList<String[]> out = new ArrayList<>();
				String url = "http://" + kvsCoord + "/workers";

				int timeout = 5000 + (attempt * 2000);
				HTTP.Response r = HTTP.doRequestWithTimeout("GET", url, null, timeout, false);

				if (r.statusCode() != 200) {
					throw new IOException("GET " + url + " -> " + r.statusCode());
				}

				String body = new String(r.body(), "UTF-8");

				for (String line : body.split("\\r?\\n")) {
					String s = line.trim();
					if (s.isEmpty()) {
						continue;
					}
					if (s.matches("^\\d+$")) {
						continue;
					}

					String addr = null, id = null;

					if (s.contains(",")) {
						String[] parts = s.split(",", 2);
						id = parts[0].trim();
						addr = parts[1].trim();
					} else {
						String[] toks = s.split("\\s+");
						if (toks.length >= 1 && toks[0].contains(":")) {
							addr = toks[0];
							if (toks.length >= 2)
								id = toks[1];
						} else if (toks.length >= 2 && toks[1].matches("^\\d+$")) {
							addr = toks[0] + ":" + toks[1];
							if (toks.length >= 3)
								id = toks[2];
						} else {
							continue;
						}
					}
					if (addr.startsWith("[") && addr.indexOf(']') > 0) {
						int rb = addr.indexOf(']');
						addr = addr.substring(1, rb) + addr.substring(rb + 1);
					}

					// get port
					int k = addr.lastIndexOf(':');
					if (k <= 0 || k == addr.length() - 1) {
						continue;
					}
					String port = addr.substring(k + 1);
					if (!port.matches("^\\d+$")) {
						continue;
					}
					out.add(new String[] { addr, id });
				}

				if (out.isEmpty()) {
					throw new IOException("No valid KVS workers parsed");
				}

				cachedKVSWorkers = out;
				lastKVSWorkerFetch = now;
				return out;

			} catch (Exception e) {
				lastException = e;
				if (attempt < MAX_KVS_FETCH_RETRIES - 1) {
					try {
						Thread.sleep(KVS_FETCH_RETRY_DELAY_MS * (attempt + 1));
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						throw new IOException("Interrupted during KVS worker fetch retry", ie);
					}
				}
			}
		}

		if (cachedKVSWorkers != null && !cachedKVSWorkers.isEmpty()) {
			System.err.println(
					"Warning: Using stale KVS worker cache after fetch failures: " + lastException.getMessage());
			return cachedKVSWorkers;
		}

		throw new IOException("Failed to fetch KVS workers after " + MAX_KVS_FETCH_RETRIES + " attempts",
				lastException);
	}

	// set concurrency
	public static void setConcurrency(int k) {
		if (k < 1) {
			concurrency = 1;
		} else {
			concurrency = k;
		}
	}

	// fold RDD
	public static String fold(String inputTable, byte[] lambdaBytes, FlamePairRDD.TwoStringsToString lambda,
			String zero) throws Exception {

		Partitioner p = new Partitioner();

		// fetch KVS workers
		List<String[]> kvsWorkers = fetchKVSWorkers();
		if (kvsWorkers.isEmpty())
			throw new RuntimeException("No KVS workers found");
		List<String[]> withIds = new ArrayList<>();
		for (String[] w : kvsWorkers) {
			String addr = w[0];
			String rawId = w[1];
			String ringId = Hasher.hash((rawId != null && !rawId.isEmpty()) ? rawId : addr);
			withIds.add(new String[] { addr, ringId });
		}
		withIds.sort(Comparator.comparing(a -> a[1]));
		int n = withIds.size();

		// get number of Flame workers
		int numFlameWorkers = instance.getWorkers().length;
		int rangesPerWorker = concurrency;

		// set number of ranges per worker
		if (numFlameWorkers > 1) {
			rangesPerWorker = Math.max(rangesPerWorker, 2);
		}
		p.setKeyRangesPerWorker(rangesPerWorker);

		// add KVS workers to partitioner
		if (n == 1) {
			// special case for single KVS worker
			String addr = withIds.get(0)[0];
			final String MID = "8000000000000000000000000000000000000000";
			p.addKVSWorker(addr, null, MID);
			p.addKVSWorker(addr, MID, null);
		} else {
			for (int i = 0; i < n; i++) {
				String addr = withIds.get(i)[0];
				String thisId = withIds.get(i)[1];
				String prevId = withIds.get((i - 1 + n) % n)[1];
				p.addKVSWorker(addr, prevId, thisId);
			}
		}

		// add Flame workers to partitioner
		for (String fw : instance.getWorkers())
			p.addFlameWorker(fw);

		// assign partitions
		Vector<Partitioner.Partition> parts = p.assignPartitions();
		if (parts == null || parts.isEmpty())
			throw new RuntimeException("Partitioning failed");
		String[] partials = new String[parts.size()];
		RuntimeException[] errs = new RuntimeException[parts.size()];
		Thread[] threads = new Thread[parts.size()];

		// create threads for each partition
		for (int i = 0; i < parts.size(); i++) {
			final int idx = i;
			threads[i] = new Thread(() -> {
				try {
					Partitioner.Partition part = parts.get(idx);
					String u = "http://" + part.assignedFlameWorker + "/rdd/fold" + "?input="
							+ URLEncoder.encode(inputTable, "UTF-8") + "&kvs=" + URLEncoder.encode(kvsCoord, "UTF-8");
					if (part.fromKey != null) {
						u += "&start=" + URLEncoder.encode(part.fromKey, "UTF-8");
					}
					if (part.toKeyExclusive != null) {
						u += "&end=" + URLEncoder.encode(part.toKeyExclusive, "UTF-8");
					}
					u += "&zero=" + URLEncoder.encode((zero == null) ? "" : zero, "UTF-8");
					HTTP.Response r = HTTP.doRequestWithTimeout("POST", u, lambdaBytes, 900000, false);
					if (r.statusCode() != 200)
						throw new RuntimeException("Worker " + part.assignedFlameWorker + " HTTP " + r.statusCode());
					partials[idx] = new String(r.body(), "UTF-8");
				} catch (Exception e) {
					errs[idx] = new RuntimeException(e);
				}
			}, "fold-" + i);
			threads[i].start();
		}

		for (Thread t : threads)
			try {
				t.join();
			} catch (InterruptedException ignored) {
			}
		for (RuntimeException e : errs)
			if (e != null)
				throw e;

		String acc = (zero == null) ? "" : zero;
		for (String s : partials) {
			if (s != null)
				acc = lambda.op(acc, s);
		}
		return acc;
	}

}
