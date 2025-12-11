package cis5550.jobs;

import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools.*;
import java.util.*;

public class PageRank {
	private static final String PAGERANK_STATE_TABLE = "pt-pageranks";
	private static final String PAGERANK_CHECKPOINT_TABLE = "pt-pagerank-checkpoint";

	public static void run(FlameContext ctx, String[] args) throws Exception {
		long totalStart = System.currentTimeMillis();

		boolean userWantsResume = args != null && args.length > 0 && args[0].equalsIgnoreCase("resume");
		int argOffset = userWantsResume ? 1 : 0;

		double threshold = 0.001;
		double percentRequired = 100.0;
		if (args != null && args.length > argOffset)
			threshold = Double.parseDouble(args[argOffset]);
		if (args != null && args.length > argOffset + 1)
			percentRequired = Double.parseDouble(args[argOffset + 1]);

		KVSClient kvs = ctx.getKVS();
		FlamePairRDD state = null;
		int startIteration = 0;
		boolean shouldResume = false;

		if (userWantsResume) {
			try {
				int stateCount = kvs.count(PAGERANK_STATE_TABLE);

				if (stateCount > 0) {
					// load checkpoint to find last iteration
					Iterator<Row> checkpointIter = kvs.scan(PAGERANK_CHECKPOINT_TABLE);
					int lastIteration = -1;
					String checkpointState = null;

					while (checkpointIter.hasNext()) {
						Row row = checkpointIter.next();
						String iterStr = row.get("iteration");
						String stateStr = row.get("state");

						if (iterStr != null) {
							try {
								int iter = Integer.parseInt(iterStr);
								if (iter > lastIteration) {
									lastIteration = iter;
									checkpointState = stateStr;
								}
							} catch (NumberFormatException e) {
								// skip invalid iteration numbers
							}
						}
					}

					if (lastIteration >= 0 && checkpointState != null) {
						shouldResume = true;
						startIteration = lastIteration;

						ctx.output(
								"[RESUME] Resuming from iteration " + lastIteration + " (" + stateCount + " nodes)\n");

						long resumeLoadStart = System.currentTimeMillis();
						// reconstruct state RDD from pt-pageranks
						FlameRDD stateRDD = ctx.fromTable(PAGERANK_STATE_TABLE, row -> {
							String urlHash = row.key();
							String rankData = row.get("rank");
							if (rankData != null && rankData.contains(",")) {
								return urlHash + "," + rankData;
							} else {
								return urlHash + "," + (rankData != null ? rankData : "1.0,1.0");
							}
						});

						state = stateRDD.flatMapToPair(s -> {
							List<FlamePair> out = new ArrayList<>();
							if (s == null)
								return out;
							try {
								int comma = s.indexOf(',');
								if (comma < 0)
									return out;
								String urlHash = s.substring(0, comma);
								String rankData = s.substring(comma + 1);
								out.add(new FlamePair(urlHash, rankData));
							} catch (Exception e) {
								System.err.println("[RESUME] Error parsing state row: " + e);
							}
							return out;
						});

						ctx.output(
								"[RESUME] State loaded (" + (System.currentTimeMillis() - resumeLoadStart) + " ms)\n");
					} else {
						ctx.output("[RESUME] No valid checkpoint found. Starting fresh.\n");
					}
				} else {
					ctx.output("[RESUME] No existing state found. Starting fresh.\n");
				}
			} catch (Exception e) {
				ctx.output("[RESUME] Error loading checkpoint: " + e.getMessage() + ". Starting fresh.\n");
				shouldResume = false;
			}
		}

		// ========================================
		// INITIALIZE IF NOT RESUMING
		// ========================================
		if (!shouldResume) {
			ctx.output("Initializing PageRank...\n");

			// -------------------------------
			// LOAD pt-crawl into an RDD safely
			// -------------------------------
			long t1 = System.currentTimeMillis();
			FlameRDD crawlData = ctx.fromTable("pt-crawl", (Row row) -> {
				try {
					String url = row.get("url");
					String page = row.get("page");

					if (url == null || page == null || page.isEmpty()) {
						System.out.println("[PageRank] Skipped invalid or empty page entry.");
						return null;
					}

					if (page.length() > 750_000) {
						return null;
					}

					return url + "," + page;
				} catch (Exception e) {
					return null;
				}
			});
			long loadTime = System.currentTimeMillis() - t1;
			ctx.output("[TIMER] Loaded crawl data in " + loadTime + " ms\n");
			System.out.println("[PageRank] Finished loading crawl data into RDD.");

			// -------------------------------
			// INITIALIZE STATE (urlHash â†’ rank info)
			// -------------------------------
			long t2 = System.currentTimeMillis();
			state = crawlData.flatMapToPair(s -> {
				List<FlamePair> out = new ArrayList<>();
				if (s == null)
					return out;

				try {
					int comma = s.indexOf(',');
					if (comma < 0)
						return out;
					String url = s.substring(0, comma).trim();
					String page = s.substring(comma + 1);

					// cap large pages again in case of truncated serialization
					if (page.length() > 750_000) {
						System.out.println("[PageRank] Skipped oversized page during flatMap for: " + url);
						return out;
					}

					// extract outbound links
					List<String> rawLinks = Crawler.extractUrls(page.getBytes());
					Set<String> normalized = new HashSet<>();
					for (String link : rawLinks) {
						String norm = Crawler.normalizeUrl(url, link);
						if (norm != null)
							normalized.add(norm);
					}

					// convert to hashed link list
					List<String> hashedLinks = new ArrayList<>();
					for (String norm : normalized) {
						hashedLinks.add(Hasher.hash(norm));
					}
					String linkList = String.join(",", hashedLinks);
					String urlHash = Hasher.hash(url);

					String value = "1.0,1.0" + (linkList.isEmpty() ? "" : "," + linkList);
					out.add(new FlamePair(urlHash, value));
				} catch (Exception e) {
					System.err.println("[PageRank] Error during initial state mapping: " + e);
				}
				return out;
			});
			long initTime = System.currentTimeMillis() - t2;
			ctx.output("[TIMER] Initialized state in " + initTime + " ms\n");

			long t3 = System.currentTimeMillis();

			// use saveAsTable  instead of collect + individual puts
			String tempInitTable = "flame-init-" + System.currentTimeMillis();
			state.saveAsTable(tempInitTable);
			
			// Copy from temp table to persistent table with "rank" column
			Iterator<Row> tempIter = kvs.scan(tempInitTable);
			int savedCount = 0;
			while (tempIter.hasNext()) {
				Row row = tempIter.next();
				String key = row.key();
				Set<String> cols = row.columns();
				if (cols != null && !cols.isEmpty()) {
					String value = row.get(cols.iterator().next());
					if (value != null) {
						kvs.put(PAGERANK_STATE_TABLE, key, "rank", value);
						savedCount++;
					}
				}
			}

			long saveInitTime = System.currentTimeMillis() - t3;

			// save initial checkpoint
			Row checkpoint = new Row("state");
			checkpoint.put("iteration", "0");
			checkpoint.put("state", "initialized");
			checkpoint.put("timestamp", Long.toString(System.currentTimeMillis()));
			kvs.putRow(PAGERANK_CHECKPOINT_TABLE, checkpoint);

			ctx.output("[INIT] Saved initial state (" + savedCount + " nodes, " + saveInitTime + " ms)\n");
			System.out.println(
					"[PageRank] Saved initial PageRank state to pt-pageranks (" + savedCount + " nodes)");
		}

		// -------------------------------
		// ITERATIVE PAGERANK COMPUTATION
		// -------------------------------
		ctx.output("Starting PageRank iterations (threshold=" + threshold + ", percentRequired=" + percentRequired
				+ "%)\n");
		System.out
				.println("[PageRank] Using threshold=" + threshold + " and percentRequired=" + percentRequired);

		boolean converged = false;
		int iteration = startIteration;
		long totalIterationTime = 0;

		// checkpoint configuration
		final int CHECKPOINT_INTERVAL = 5;
		long lastCheckpointTime = System.currentTimeMillis();
		final long CHECKPOINT_TIME_MS = 15 * 60 * 1000;

		while (!converged) {
			iteration++;
			long iterStart = System.currentTimeMillis();
			ctx.output("=== Iteration " + iteration + " ===\n");
			System.out.println("[PageRank] Starting iteration " + iteration + "...");

			// -------------------------------
			// COMPUTE TRANSFERS
			// -------------------------------
			FlamePairRDD transferTable = state.flatMapToPair(pair -> {
				List<FlamePair> out = new ArrayList<>();
				try {
					String key = pair._1();
					String[] parts = pair._2().split(",");
					if (parts.length < 2)
						return out;

					double rc = Double.parseDouble(parts[0]);
					// count links first then iterate once instead of building list
					int linkCount = 0;
					int firstLinkIdx = -1;
					for (int i = 2; i < parts.length; i++) {
						if (!parts[i].isEmpty()) {
							if (firstLinkIdx == -1) firstLinkIdx = i;
							linkCount++;
						}
					}

					double d = 0.85;
					if (linkCount > 0) {
						// pre-compute string once and iterate once to add all links
						double share = d * rc / linkCount;
						String shareStr = String.valueOf(share);
						for (int i = 2; i < parts.length; i++) {
							if (!parts[i].isEmpty()) {
								out.add(new FlamePair(parts[i], shareStr));
							}
						}
					}

					out.add(new FlamePair(key, "0.0"));
				} catch (Exception e) {
					System.err.println("[PageRank] Error in transferTable: " + e);
				}
				return out;
			});

			FlamePairRDD aggregated = transferTable.foldByKey("0.0", (a, b) -> {
				try {
					double sum = Double.parseDouble(a) + Double.parseDouble(b);
					if (sum == 0.0) return "0.0";
					return String.valueOf(sum);
				} catch (Exception e) {
					return a;
				}
			});

			System.out.println("[PageRank] Aggregated contributions for iteration " + iteration);

			// -------------------------------
			// UPDATE RANKS & COMPUTE DIFF
			// -------------------------------
			FlamePairRDD joined = state.join(aggregated);

			FlamePairRDD updatedState = joined.flatMapToPair(pair -> {
				List<FlamePair> out = new ArrayList<>();
				try {
					String key = pair._1();
					String[] parts = pair._2().split(",");
					if (parts.length < 3)
						return out;

					double oldCurr = Double.parseDouble(parts[0]);
					double newRank = Double.parseDouble(parts[parts.length - 1]);

					double d = 0.85;
					double updatedRank = newRank + (1 - d);

					// use StringBuilder to efficiently build the string
					StringBuilder newVal = new StringBuilder();
					newVal.append(updatedRank).append(",").append(oldCurr);
					
					// Only add links if they exist
					if (parts.length > 3) {
						for (int i = 2; i < parts.length - 1; i++) {
							if (!parts[i].isEmpty()) {
								newVal.append(",").append(parts[i]);
							}
						}
					}

					out.add(new FlamePair(key, newVal.toString()));
				} catch (Exception e) {
					System.err.println("[PageRank] Error updating rank: " + e);
				}
				return out;
			});

			// OPTIMIZATION: Compute diff more efficiently
			FlameRDD diffs = updatedState.flatMap(pair -> {
				List<String> out = new ArrayList<>();
				try {
					String value = pair._2();
					int commaIdx = value.indexOf(',');
					if (commaIdx < 0) return out;
					
					double curr = Double.parseDouble(value.substring(0, commaIdx));
					// Find second comma (or end) for prev value
					int commaIdx2 = value.indexOf(',', commaIdx + 1);
					if (commaIdx2 < 0) commaIdx2 = value.length();
					double prev = Double.parseDouble(value.substring(commaIdx + 1, commaIdx2));
					
					double diff = Math.abs(curr - prev);
					// Use faster string conversion
					out.add(diff == 0.0 ? "0.0" : String.valueOf(diff));
				} catch (Exception e) {
					System.err.println("[PageRank] Error computing diff: " + e);
				}
				return out;
			});

			final double thresholdFinal = threshold;
			// OPTIMIZATION: More efficient fold with reduced string operations
			String combinedStats = diffs.fold("0.0,0,0", (a, b) -> {
				try {
					// Parse accumulator once
					int comma1 = a.indexOf(',');
					int comma2 = a.indexOf(',', comma1 + 1);
					if (comma1 < 0 || comma2 < 0) return a;
					
					double maxDiffA = Double.parseDouble(a.substring(0, comma1));
					int convA = Integer.parseInt(a.substring(comma1 + 1, comma2));
					int totalA = Integer.parseInt(a.substring(comma2 + 1));

					// Check if b is a single diff value or combined stats
					int commaB = b.indexOf(',');
					if (commaB < 0) {
						// Single diff value
						double diffB = Double.parseDouble(b);
						double newMaxDiff = Math.max(maxDiffA, diffB);
						int newConv = convA + (diffB <= thresholdFinal ? 1 : 0);
						int newTotal = totalA + 1;
						return newMaxDiff + "," + newConv + "," + newTotal;
					} else {
						// Combined stats to parse once
						int commaB2 = b.indexOf(',', commaB + 1);
						if (commaB2 < 0) return a;
						double maxDiffB = Double.parseDouble(b.substring(0, commaB));
						int convB = Integer.parseInt(b.substring(commaB + 1, commaB2));
						int totalB = Integer.parseInt(b.substring(commaB2 + 1));

						double newMaxDiff = Math.max(maxDiffA, maxDiffB);
						int newConv = convA + convB;
						int newTotal = totalA + totalB;
						return newMaxDiff + "," + newConv + "," + newTotal;
					}
				} catch (Exception e) {
					return a;
				}
			});

			String[] combinedParts = combinedStats.split(",");
			double maxDiff = Double.parseDouble(combinedParts[0]);
			int convergedCount = Integer.parseInt(combinedParts[1]);
			int total = Integer.parseInt(combinedParts[2]);

			if (total == 0) {
				ctx.output("[ERROR] No nodes processed in iteration " + iteration + ". Continuing...\n");
				state = updatedState;
				continue;
			}

			double percentConverged = 100.0 * convergedCount / total;
			ctx.output(String.format("Iteration %d: MaxDiff=%.6f, %.2f%% converged (need %.2f%%)\n", iteration, maxDiff,
					percentConverged, percentRequired));
			System.out.println(
					String.format("[PageRank] Iteration %d: MaxDiff=%.15f | Converged: %.1f%% / %.1f%% target",
							iteration, maxDiff, percentConverged, percentRequired));

			state = updatedState;

			// save checkpoint periodically
			boolean shouldCheckpoint = false;
			long now = System.currentTimeMillis();

			// checkpoint iterations
			if (iteration % CHECKPOINT_INTERVAL == 0) {
				shouldCheckpoint = true;
			}

			// checkpoint based on time
			if (now - lastCheckpointTime >= CHECKPOINT_TIME_MS) {
				shouldCheckpoint = true;
			}

			if (shouldCheckpoint) {
				try {
					long checkpointStart = System.currentTimeMillis();

					// save to temp table using saveAsTable
					// then copy to pt-pageranks with correct column name "rank"
					String tempTable = "flame-checkpoint-" + System.currentTimeMillis();
					state.saveAsTable(tempTable);

					// copy from temp table to persistent table with "rank" column
					Iterator<Row> tempIter = kvs.scan(tempTable);
					int checkpointCount = 0; // count the number of rows copied to the checkpoint table
					while (tempIter.hasNext()) {
						Row row = tempIter.next();
						String key = row.key();
						Set<String> cols = row.columns();
						if (cols != null && !cols.isEmpty()) {
							String value = row.get(cols.iterator().next());
							if (value != null) {
								kvs.put(PAGERANK_STATE_TABLE, key, "rank", value);
								checkpointCount++;
							}
						}
					}

					// verify checkpoint
					if (checkpointCount == 0) {
						ctx.output("[WARNING] No rows copied during checkpoint!\n"); // if warning, then no rows were copied to the checkpoint table
					}

					// save checkpoint
					Row checkpoint = new Row("state");
					checkpoint.put("iteration", String.valueOf(iteration));
					checkpoint.put("state", "iterating");
					checkpoint.put("maxDiff", String.valueOf(maxDiff));
					checkpoint.put("percentConverged", String.valueOf(percentConverged));
					checkpoint.put("timestamp", Long.toString(now));
					kvs.putRow(PAGERANK_CHECKPOINT_TABLE, checkpoint);

					ctx.output("[CHECKPOINT] Saved after iteration " + iteration + " ("
							+ (System.currentTimeMillis() - checkpointStart) / 1000 + "s)\n");
					System.out.println("[PageRank] [CHECKPOINT] Saved state after iteration " + iteration);

					lastCheckpointTime = now;
				} catch (Exception e) {
					ctx.output("[WARNING] Failed to save checkpoint: " + e.getMessage() + "\n");
				}

			}

			if (maxDiff < threshold || percentConverged >= percentRequired) {
				converged = true;
				ctx.output("Converged after " + iteration + " iterations.\n");

				// save final checkpoint
				Row checkpoint = new Row("state");
				checkpoint.put("iteration", String.valueOf(iteration));
				checkpoint.put("state", "converged");
				checkpoint.put("timestamp", Long.toString(System.currentTimeMillis()));
				kvs.putRow(PAGERANK_CHECKPOINT_TABLE, checkpoint);
			}

			long iterTime = System.currentTimeMillis() - iterStart;
			totalIterationTime += iterTime;
			ctx.output("[TIMER] Iteration " + iteration + ": " + (iterTime / 1000) + "s\n");
		}

		ctx.output(
				"[TIMER] Total iteration time: " + (totalIterationTime / 1000) + "s (" + iteration + " iterations)\n");

		// -------------------------------
		// SAVE FINAL RANKS
		// -------------------------------
		long t4 = System.currentTimeMillis();
		ctx.output("Saving final ranks...\n");

		// create RDD with current rank, then saveAsTable
		FlamePairRDD finalRanks = state.flatMapToPair(pair -> {
			List<FlamePair> out = new ArrayList<>();
			try {
				String key = pair._1();
				String[] parts = pair._2().split(",");
				if (parts.length >= 1) {
					out.add(new FlamePair(key, parts[0]));
				}
			} catch (Exception e) {
				// skip errors
			}
			return out;
		});

		// save to temp table, then copy with "rank" column
		String tempFinalTable = "flame-final-" + System.currentTimeMillis();
		finalRanks.saveAsTable(tempFinalTable);

		// copy from temp table to persistent table with "rank" column
		Iterator<Row> tempIter = ctx.getKVS().scan(tempFinalTable);
		while (tempIter.hasNext()) {
			Row row = tempIter.next();
			String key = row.key();
			Set<String> cols = row.columns();
			if (cols != null && !cols.isEmpty()) {
				String rank = row.get(cols.iterator().next());
				if (rank != null) {
					ctx.getKVS().put(PAGERANK_STATE_TABLE, key, "rank", rank);
				}
			}
		}


		long saveFinalTime = System.currentTimeMillis() - t4;
		long totalTime = System.currentTimeMillis() - totalStart;
		ctx.output("[TIMER] Saved final ranks in " + (saveFinalTime / 1000) + "s\n");
		ctx.output("[TIMER] TOTAL runtime: " + (totalTime / 1000) + "s\n");
	}
}
