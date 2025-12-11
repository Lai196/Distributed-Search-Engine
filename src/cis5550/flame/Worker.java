package cis5550.flame;

import java.util.*;
import java.net.*;
import java.io.*;
import java.util.concurrent.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.kvs.*;
import cis5550.webserver.Request;

class Worker extends cis5550.generic.Worker {

	public static void main(String args[]) {
		if (args.length != 2) {
			System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
			System.exit(1);
		}

		int port = Integer.parseInt(args[0]);
		String server = args[1];
		Worker worker = new Worker();
		worker.coordinatorAddr = server;
		worker.httpPort = port;
		worker.workerId = Worker.randomId5();
		worker.startPingThread();
		File jarsDir = new File("jar");
		jarsDir.mkdirs();
		final File myJAR = new File(jarsDir, "__worker" + port + "-current.jar");
		port(port);

		// Thread pool for parallel KVS writes
		int threadPoolSize = 16;
		String threadsProp = System.getProperty("flame.worker.threads");
		if (threadsProp != null && !threadsProp.isEmpty()) {
			try {
				threadPoolSize = Integer.parseInt(threadsProp);
				if (threadPoolSize < 1)
					threadPoolSize = 16;
			} catch (NumberFormatException e) {
				System.err.println("Warning: Invalid flame.worker.threads value, using default 16");
			}
		}
		final ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize);
		System.out.println("[Flame Worker] Thread pool size: " + threadPoolSize);

		// post request to use JAR
		post("/useJAR", (request, response) -> {
			FileOutputStream fos = new FileOutputStream(myJAR);
			fos.write(request.bodyAsBytes());
			fos.close();
			return "OK";
		});

		// post request to flatMap
		post("/rdd/flatMap", (request, response) -> {
			try {
				String input = request.queryParams("input");
				String output = request.queryParams("output");
				String kvsC = request.queryParams("kvs");
				String start = request.queryParams("start");
				String end = request.queryParams("end");

				FlameRDD.StringToIterable f = (FlameRDD.StringToIterable) Serializer
						.byteArrayToObject(request.bodyAsBytes(), myJAR);
				long startTime = System.currentTimeMillis();
				long timeoutMs = 700000;
				long loopIterations = 0;

				KVSClient kvs = new KVSClient(kvsC);
				Iterator<Row> it = null;
				try {
					it = kvs.scan(input);
				} catch (Exception scanEx) {
					System.err.println("Warning: KVS scan failed in flatMap: " + scanEx.getMessage());
					return "OK";
				}

				if (it == null) {
					return "OK";
				}

				// process each row
				while (it.hasNext()) {
					if (loopIterations % 100 == 0) {
						long elapsed = System.currentTimeMillis() - startTime;
						if (elapsed > timeoutMs) {
							System.err.println("Warning: flatMap processing timeout after " + elapsed
									+ "ms, preserving unprocessed rows and returning early");
							long preserved = 0;
							long preserveStartTime = System.currentTimeMillis();
							long maxPreserveTime = 60000;

							// preserve unprocessed rows
							while (it.hasNext() && (System.currentTimeMillis() - preserveStartTime) < maxPreserveTime) {
								try {
									Row r = it.next();
									if (r != null) {
										String k = r.key();
										if (keyInPartition(k, start, end)) {
											String val = r.get("value");
											if (val != null) {
												try {
													kvs.put(output, k + ":0", "value", val);
													preserved++;
												} catch (Exception e) {
												}
											}
										}
									}
								} catch (Exception e) {
									break;
								}
							}
							if (preserved > 0) {
								System.err.println("Preserved " + preserved + " unprocessed rows for next iteration");
							}
							return "OK";
						}
					}
					loopIterations++;

					// get next row
					Row r = null;
					try {
						r = it.next();
					} catch (Exception iterEx) {
						System.err.println("Warning: Iterator failed in flatMap: " + iterEx.getMessage());
						continue;
					}
					if (r == null) {
						continue;
					}
					String k = r.key();
					if (!keyInPartition(k, start, end)) {
						continue;
					}

					Iterable<String> outs = null;
					try {
						outs = f.op(r.get("value"));
					} catch (Exception opEx) {
						System.err.println("Warning: User lambda failed in flatMap: " + opEx.getMessage());
						continue;
					}
					if (outs == null) {
						continue;
					}

					// process each output
					long idx = 0;
					List<Callable<Boolean>> tasks = new ArrayList<>();
					for (String s : outs) {
						if (s == null) {
							continue;
						}
						final String finalK = k;
						final long finalIdx = idx++;
						final String finalS = s;

						tasks.add(() -> {
							boolean saved = false;
							// retry if save fails
							for (int retry = 0; retry < 2 && !saved; retry++) {
								try {
									kvs.put(output, finalK + ":" + finalIdx, "value", finalS);
									saved = true;
								} catch (Exception putEx) {
									String errorMsg = putEx.getMessage();
									if (retry == 0 && errorMsg != null
											&& (errorMsg.contains("Connection") || errorMsg.contains("refused")
													|| errorMsg.contains("timeout")
													|| errorMsg.contains("Address already in use"))) {
										try {
											Thread.sleep(100);
										} catch (InterruptedException ie) {
											break;
										}
									} else {
										break;
									}
								}
							}
							return saved;
						});
					}

					if (!tasks.isEmpty()) {
						try {
							List<Future<Boolean>> futures = threadPool.invokeAll(tasks);
							for (Future<Boolean> future : futures) {
								future.get();
							}
						} catch (Exception e) {
							System.err.println("Error in parallel writes: " + e.getMessage());
						}
					}
				}

				return "OK";
			} catch (Exception e) {
				System.err.println("Error in flatMap handler: " + e.getMessage());
				e.printStackTrace();
				return "OK";
			}
		});

		// post request to mapToPair
		post("/rdd/mapToPair", (request, response) -> {
			String input = request.queryParams("input");
			String output = request.queryParams("output");
			String kvsC = request.queryParams("kvs");
			String start = request.queryParams("start");
			String end = request.queryParams("end");

			FlameRDD.StringToPair f = (FlameRDD.StringToPair) Serializer.byteArrayToObject(request.bodyAsBytes(),
					myJAR);

			KVSClient kvs = new KVSClient(kvsC);
			Iterator<Row> it = kvs.scan(input);
			while (it.hasNext()) {
				Row r = it.next();
				String k = r.key();
				if (!keyInPartition(k, start, end)) {
					continue;
				}
				FlamePair p = f.op(r.get("value"));
				if (p == null) {
					continue;
				}
				String col = UUID.randomUUID().toString();
				kvs.put(output, p._1(), col, p._2());
			}
			return "OK";
		});

		// post request to foldByKey
		post("/pair/foldByKey", (request, response) -> {
			String input = request.queryParams("input");
			String output = request.queryParams("output");
			String kvsC = request.queryParams("kvs");
			String start = request.queryParams("start");
			String end = request.queryParams("end");
			String zero = request.queryParams("zero");
			if (zero == null)
				zero = "";

			Object lambda = Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

			KVSClient kvs = new KVSClient(kvsC);
			Iterator<Row> it = kvs.scan(input);
			while (it.hasNext()) {
				Row r = it.next();
				String k = r.key();
				if (!keyInPartition(k, start, end)) {
					continue;
				}

				Set<String> cols = r.columns();
				String acc = zero;

				if (lambda instanceof FlamePairRDD.StringAccumulator) {
					FlamePairRDD.StringAccumulator f = (FlamePairRDD.StringAccumulator) lambda;
					StringBuilder sb = new StringBuilder();
					if (zero != null && !zero.isEmpty())
						sb.append(zero);

					if (cols != null) {
						for (String c : cols) {
							String v = r.get(c);
							if (v != null)
								f.op(sb, v);
						}
					}
					acc = sb.toString();
				} else {
					FlamePairRDD.TwoStringsToString f = (FlamePairRDD.TwoStringsToString) lambda;
					if (cols != null) {
						for (String c : cols) {
							String v = r.get(c);
							if (v != null)
								acc = f.op(acc, v);
						}
					}
				}
				kvs.put(output, k, "value", acc);
			}

			return "OK";
		});

		// post request to intersection
		post("/rdd/intersection", (request, response) -> {
			String input1 = request.queryParams("input");
			String input2 = request.queryParams("other");
			String output = request.queryParams("output");
			String kvsC = request.queryParams("kvs");
			String start = request.queryParams("start");
			String end = request.queryParams("end");

			KVSClient kvs = new KVSClient(kvsC);

			HashSet<String> inB = new HashSet<>();
			Iterator<Row> it2 = kvs.scan(input2);
			while (it2.hasNext()) {
				Row r2 = it2.next();
				String v2 = r2.get("value");
				if (v2 != null) {
					inB.add(v2);
				}
			}

			Iterator<Row> it1 = kvs.scan(input1);
			while (it1.hasNext()) {
				Row r1 = it1.next();
				String k1 = r1.key();
				if (!keyInPartition(k1, start, end))
					continue;
				String v1 = r1.get("value");
				if (v1 == null) {
					continue;
				}
				if (inB.contains(v1)) {
					kvs.put(output, v1, "value", v1);
				}
			}
			return "OK";
		});

		// post request to sample
		post("/rdd/sample", (request, response) -> {
			try {
				String input = request.queryParams("input");
				String output = request.queryParams("output");
				String kvsC = request.queryParams("kvs");
				String start = request.queryParams("start");
				String end = request.queryParams("end");
				String pParam = request.queryParams("p");

				if (pParam == null) {
					response.status(400, "Bad Request");
					return "Missing parameter: p";
				}

				double p = Double.parseDouble(pParam);

				if (p <= 0.0) {
					return "OK";
				}
				if (p >= 1.0) {
					KVSClient kvsAll = new KVSClient(kvsC);
					Iterator<Row> itAll = kvsAll.scan(input);
					long idxAll = 0;
					while (itAll.hasNext()) {
						Row r = itAll.next();
						if (!keyInPartition(r.key(), start, end))
							continue;
						String v = r.get("value");
						if (v != null)
							kvsAll.put(output, r.key() + ":" + (idxAll++), "value", v);
					}
					return "OK";
				}

				KVSClient kvs = new KVSClient(kvsC);
				Iterator<Row> it = kvs.scan(input);
				long idx = 0;
				while (it.hasNext()) {
					Row r = it.next();
					String k = r.key();
					if (!keyInPartition(k, start, end))
						continue;
					String v = r.get("value");
					if (v == null)
						continue;

					String h = Hasher.hash(v);
					long hi = Long.parseUnsignedLong(h.substring(0, 16), 16);
					double u = ((hi >>> 11) * (1.0 / (1L << 53)));

					if (u < p) {
						kvs.put(output, k + ":" + (idx++), "value", v);
					}
				}
				return "OK";
			} catch (Exception e) {
				e.printStackTrace();
				response.status(500, "Internal Server Error");
				return "Error: " + e.getMessage();
			}
		});

		// post request to fromTable
		post("/ctx/fromTable", (request, response) -> {
			String input = request.queryParams("input");
			String output = request.queryParams("output");
			String kvsC = request.queryParams("kvs");
			String start = request.queryParams("start");
			String end = request.queryParams("end");

			FlameContext.RowToString f = (FlameContext.RowToString) Serializer.byteArrayToObject(request.bodyAsBytes(),
					myJAR);

			KVSClient kvs = new KVSClient(kvsC);
			Iterator<Row> it = kvs.scan(input);
			while (it.hasNext()) {
				Row r = it.next();
				String k = r.key();
				if (!keyInPartition(k, start, end)) {
					continue;
				}

				String v = f.op(r);
				if (v != null) {
					kvs.put(output, k, "value", v);
				}
			}
			return "OK";
		});

		// post request to flatMapToPair - OPTIMIZED WITH BATCHING
		post("/rdd/flatMapToPair", (request, response) -> {
			String input = request.queryParams("input");
			String output = request.queryParams("output");
			String kvsC = request.queryParams("kvs");
			String start = request.queryParams("start");
			String end = request.queryParams("end");

			FlameRDD.StringToPairIterable f = (FlameRDD.StringToPairIterable) Serializer
					.byteArrayToObject(request.bodyAsBytes(), myJAR);

			KVSClient kvs = new KVSClient(kvsC);
			Iterator<Row> it = kvs.scan(input);
			while (it.hasNext()) {
				Row row = it.next();
				String k = row.key();
				if (!keyInPartition(k, start, end)) {
					continue;
				}
				String val = row.get("value");
				if (val == null) {
					continue;
				}

				Iterable<FlamePair> outs = f.op(val);
				if (outs == null) {
					continue;
				}

				// batch pairs by key
				Map<String, Row> batchedRows = new HashMap<>();
				for (FlamePair p : outs) {
					if (p == null) {
						continue;
					}
					String pairKey = p._1();
					String col = UUID.randomUUID().toString();
					Row batchRow = batchedRows.computeIfAbsent(pairKey, k2 -> new Row(k2));
					batchRow.put(col, p._2());
				}

				// write batched rows using put() instead of putRow()
				// putRow() overwrites the entire row, must use individual put() so pt-table
				// will work
				for (Row batchRow : batchedRows.values()) {
					String rowKey = batchRow.key();
					Set<String> cols = batchRow.columns();
					if (cols != null) {
						for (String col : cols) {
							String colValue = batchRow.get(col);
							if (colValue != null) {
								try {
									kvs.put(output, rowKey, col, colValue);
								} catch (Exception e) {
									System.err.println("Error writing column: " + e.getMessage());
								}
							}
						}
					}
				}
			}
			return "OK";
		});

		// post request to flatMap
		post("/pair/flatMap", (request, response) -> {
			String input = request.queryParams("input");
			String output = request.queryParams("output");
			String kvsC = request.queryParams("kvs");
			String start = request.queryParams("start");
			String end = request.queryParams("end");

			FlamePairRDD.PairToStringIterable f = (FlamePairRDD.PairToStringIterable) Serializer
					.byteArrayToObject(request.bodyAsBytes(), myJAR);

			KVSClient kvs = new KVSClient(kvsC);
			Iterator<Row> it = kvs.scan(input);
			while (it.hasNext()) {
				Row row = it.next();
				String key = row.key();
				if (!keyInPartition(key, start, end)) {
					continue;
				}

				Set<String> cols = row.columns();
				if (cols == null) {
					continue;
				}
				long idx = 0;
				for (String c : cols) {
					String val = row.get(c);
					if (val == null) {
						continue;
					}

					Iterable<String> outs = f.op(new FlamePair(key, val));
					if (outs == null) {
						continue;
					}

					List<Callable<Boolean>> tasks = new ArrayList<>();
					for (String s : outs) {
						if (s == null) {
							continue;
						}
						final String finalKey = key;
						final long finalIdx = idx++;
						final String finalS = s;
						tasks.add(() -> {
							kvs.put(output, finalKey + ":" + finalIdx, "value", finalS);
							return true;
						});
					}

					if (!tasks.isEmpty()) {
						try {
							List<Future<Boolean>> futures = threadPool.invokeAll(tasks);
							for (Future<Boolean> future : futures) {
								future.get();
							}
						} catch (Exception e) {
							System.err.println("Error in parallel writes (pair/flatMap): " + e.getMessage());
						}
					}
				}
			}
			return "OK";
		});

		// post request to flatMapToPair
		post("/pair/flatMapToPair", (request, response) -> {
			String input = request.queryParams("input");
			String output = request.queryParams("output");
			String kvsC = request.queryParams("kvs");
			String start = request.queryParams("start");
			String end = request.queryParams("end");

			FlamePairRDD.PairToPairIterable f = (FlamePairRDD.PairToPairIterable) Serializer
					.byteArrayToObject(request.bodyAsBytes(), myJAR);

			KVSClient kvs = new KVSClient(kvsC);
			Iterator<Row> it = kvs.scan(input);
			while (it.hasNext()) {
				Row row = it.next();
				String key = row.key();
				if (!keyInPartition(key, start, end)) {
					continue;
				}

				Set<String> cols = row.columns();
				if (cols == null) {
					continue;
				}
				for (String c : cols) {
					String val = row.get(c);
					if (val == null) {
						continue;
					}

					Iterable<FlamePair> outs = f.op(new FlamePair(key, val));
					if (outs == null) {
						continue;
					}

					List<Callable<Boolean>> tasks = new ArrayList<>();
					for (FlamePair p : outs) {
						if (p == null) {
							continue;
						}
						final FlamePair finalP = p;
						tasks.add(() -> {
							String col = UUID.randomUUID().toString();
							kvs.put(output, finalP._1(), col, finalP._2());
							return true;
						});
					}

					if (!tasks.isEmpty()) {
						try {
							List<Future<Boolean>> futures = threadPool.invokeAll(tasks);
							for (Future<Boolean> future : futures) {
								future.get();
							}
						} catch (Exception e) {
							System.err.println("Error in parallel writes (pair/flatMapToPair): " + e.getMessage());
						}
					}
				}
			}
			return "OK";
		});

		// post request to filter
		post("/rdd/filter", (request, response) -> {
			String input = request.queryParams("input");
			String output = request.queryParams("output");
			String kvsC = request.queryParams("kvs");
			String start = request.queryParams("start");
			String end = request.queryParams("end");

			FlameRDD.StringToBoolean f = (FlameRDD.StringToBoolean) Serializer.byteArrayToObject(request.bodyAsBytes(),
					myJAR);

			KVSClient kvs = new KVSClient(kvsC);
			Iterator<Row> it = kvs.scan(input);
			while (it.hasNext()) {
				Row row = it.next();
				String k = row.key();
				if (!keyInPartition(k, start, end)) {
					continue;
				}

				String val = row.get("value");
				if (val == null) {
					continue;
				}

				if (f.op(val)) {
					kvs.put(output, k, "value", val);
				}
			}
			return "OK";
		});

		// post request to mapPartitions
		post("/rdd/mapPartitions", (request, response) -> {
			String input = request.queryParams("input");
			String output = request.queryParams("output");
			String kvsC = request.queryParams("kvs");
			String start = request.queryParams("start");
			String end = request.queryParams("end");

			FlameRDD.IteratorToIterator f = (FlameRDD.IteratorToIterator) Serializer
					.byteArrayToObject(request.bodyAsBytes(), myJAR);

			KVSClient kvs = new KVSClient(kvsC);

			ArrayList<String> part = new ArrayList<>();
			Iterator<Row> it = kvs.scan(input);
			while (it.hasNext()) {
				Row row = it.next();
				String k = row.key();
				if (!keyInPartition(k, start, end)) {
					continue;
				}

				String val = row.get("value");
				if (val != null) {
					part.add(val);
				}
			}

			Iterator<String> out = f.op(part.iterator());
			if (out != null) {
				while (out.hasNext()) {
					String s = out.next();
					if (s == null)
						continue;
					kvs.put(output, UUID.randomUUID().toString(), "value", s);
				}
			}
			return "OK";
		});

		// post request to distinct
		post("/rdd/distinct", (request, response) -> {
			try {
				String input = request.queryParams("input");
				String output = request.queryParams("output");
				String kvsC = request.queryParams("kvs");
				String start = request.queryParams("start");
				String end = request.queryParams("end");

				KVSClient kvs = new KVSClient(kvsC);
				Iterator<Row> it = null;
				try {
					it = kvs.scan(input);
				} catch (Exception scanEx) {
					System.err.println("Warning: KVS scan failed in distinct: " + scanEx.getMessage());
					return "OK";
				}

				if (it == null) {
					return "OK";
				}

				while (it.hasNext()) {
					Row row = null;
					try {
						row = it.next();
					} catch (Exception iterEx) {
						System.err.println("Warning: Iterator failed in distinct: " + iterEx.getMessage());
						continue;
					}
					if (row == null) {
						continue;
					}

					String k = row.key();
					if (!keyInPartition(k, start, end)) {
						continue;
					}

					String val = row.get("value");
					if (val == null) {
						continue;
					}

					boolean saved = false;
					for (int retry = 0; retry < 2 && !saved; retry++) {
						try {
							kvs.put(output, val, "value", val);
							saved = true;
						} catch (Exception putEx) {
							String errorMsg = putEx.getMessage();
							if (retry == 0 && errorMsg != null
									&& (errorMsg.contains("Connection") || errorMsg.contains("refused")
											|| errorMsg.contains("timeout")
											|| errorMsg.contains("Address already in use"))) {
								try {
									Thread.sleep(100);
								} catch (InterruptedException ie) {
									break;
								}
							} else {
								break;
							}
						}
					}
				}
				return "OK";
			} catch (Exception e) {
				System.err.println("Error in distinct handler: " + e.getMessage());
				e.printStackTrace();
				return "OK";
			}
		});

		// post request to join
		post("/pair/join", (request, response) -> {
			String input = request.queryParams("input");
			String other = request.queryParams("other");
			String output = request.queryParams("output");
			String kvsC = request.queryParams("kvs");
			String start = request.queryParams("start");
			String end = request.queryParams("end");

			KVSClient kvs = new KVSClient(kvsC);

			Iterator<Row> it = kvs.scan(input);
			while (it.hasNext()) {
				Row rowA = it.next();
				String key = rowA.key();
				if (!keyInPartition(key, start, end)) {
					continue;
				}

				Row rowB = kvs.getRow(other, key);
				if (rowB == null) {
					continue;
				}

				Set<String> colsA = rowA.columns();
				Set<String> colsB = rowB.columns();
				if (colsA == null || colsB == null) {
					continue;
				}

				for (String colA : colsA) {
					String valA = rowA.get(colA);
					if (valA == null) {
						continue;
					}
					for (String colB : colsB) {
						String valB = rowB.get(colB);
						if (valB == null) {
							continue;
						}
						String outCol = UUID.randomUUID().toString();
						kvs.put(output, key, outCol, valA + "," + valB);
					}
				}
			}
			return "OK";
		});

		// post request to fold
		post("/rdd/fold", (request, response) -> {
			String input = request.queryParams("input");
			String kvsC = request.queryParams("kvs");
			String start = request.queryParams("start");
			String end = request.queryParams("end");
			String zero = request.queryParams("zero");
			if (zero == null) {
				zero = "";
			}

			FlamePairRDD.TwoStringsToString f = (FlamePairRDD.TwoStringsToString) Serializer
					.byteArrayToObject(request.bodyAsBytes(), myJAR);

			KVSClient kvs = new KVSClient(kvsC);
			String res = zero;

			for (Iterator<Row> it = kvs.scan(input); it.hasNext();) {
				Row r = it.next();
				String key = r.key();
				if (!keyInPartition(key, start, end)) {
					continue;
				}
				String val = r.get("value");
				if (val != null) {
					res = f.op(res, val);
				}
			}
			if (res == null) {
				return "";
			} else {
				return res;
			}
		});

		// post request to cogroup
		post("/pair/cogroup", (request, response) -> {
			String input = request.queryParams("input");
			String other = request.queryParams("other");
			String output = request.queryParams("output");
			String kvsC = request.queryParams("kvs");
			String start = request.queryParams("start");
			String end = request.queryParams("end");

			KVSClient kvs = new KVSClient(kvsC);
			HashSet<String> seen = new HashSet<>();

			for (Iterator<Row> it = kvs.scan(input); it.hasNext();) {
				Row a = it.next();
				String k = a.key();
				if (!keyInPartition(k, start, end)) {
					continue;
				}

				ArrayList<String> A = new ArrayList<>();
				Set<String> aCols = a.columns();
				if (aCols != null) {
					for (String c : aCols) {
						String v = a.get(c);
						if (v != null) {
							A.add(v);
						}
					}
				}
				Collections.sort(A);

				Row b = kvs.getRow(other, k);
				ArrayList<String> B = new ArrayList<>();
				if (b != null) {
					Set<String> bCols = b.columns();
					if (bCols != null) {
						for (String c : bCols) {
							String v = b.get(c);
							if (v != null) {
								B.add(v);
							}
						}
					}
				}
				Collections.sort(B);

				String stringA = "[" + String.join(",", A) + "]";
				String stringB = "[" + String.join(",", B) + "]";
				kvs.put(output, k, "value", stringA + "," + stringB);
				seen.add(k);
			}

			for (Iterator<Row> it = kvs.scan(other); it.hasNext();) {
				Row b = it.next();
				String k = b.key();
				if (!keyInPartition(k, start, end)) {
					continue;
				}
				if (seen.contains(k)) {
					continue;
				}

				ArrayList<String> B = new ArrayList<>();
				Set<String> bCols = b.columns();
				if (bCols != null) {
					for (String c : bCols) {
						String v = b.get(c);
						if (v != null) {
							B.add(v);
						}
					}
				}
				Collections.sort(B);

				String stringA = "[]";
				String stringB = "[" + String.join(",", B) + "]";
				kvs.put(output, k, "value", stringA + "," + stringB);
			}
			return "OK";
		});
	}

	private static boolean keyInPartition(String rowKey, String start, String end) {
		String h = Hasher.hash(rowKey).split("\n")[0];
		if (start == null && end == null) {
			return true;
		} else if (start == null) {
			return h.compareTo(end) <= 0;
		} else if (end == null) {
			return h.compareTo(start) > 0;
		} else {
			int cmp = start.compareTo(end);
			if (cmp < 0) {
				return h.compareTo(start) > 0 && h.compareTo(end) <= 0;
			} else if (cmp > 0) {
				return h.compareTo(start) > 0 || h.compareTo(end) <= 0;
			} else {
				return true;
			}
		}
	}
}
