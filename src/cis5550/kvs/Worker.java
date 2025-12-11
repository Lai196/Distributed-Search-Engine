package cis5550.kvs;

import static cis5550.webserver.Server.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

import cis5550.tools.HTTP;
import cis5550.tools.KeyEncoder;

import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class Worker extends cis5550.generic.Worker {
	private static final ConcurrentMap<String, ConcurrentMap<String, NavigableMap<Integer, Row>>> versions = new ConcurrentHashMap<>();
	private String storageDir;
	private static final ConcurrentMap<String, String> workersById = new ConcurrentHashMap<>();
	private static volatile List<String> workerList = List.of();

	// Row-level locks to prevent race conditions during concurrent column writes
	private static final ConcurrentHashMap<String, Object> rowLocks = new ConcurrentHashMap<>();

	// Cache for pt-table rows
	private static final ConcurrentHashMap<String, Row> ptTableCache = new ConcurrentHashMap<>();

	private static Object getRowLock(String table, String rowKey) {
		String lockKey = table + ":" + rowKey;
		return rowLocks.computeIfAbsent(lockKey, k -> new Object());
	}

	private static String getCacheKey(String table, String rowKey) {
		return table + ":" + rowKey;
	}

	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Error: Reruired argument  <httpPort> <storageDir> <coordinatorHost:port>");
			System.exit(1);
		}
		final int httpPort = Integer.parseInt(args[0]);
		port(httpPort);
		Worker self = new Worker();
		self.httpPort = httpPort;
		self.workerId = getWorkerId(args[1], httpPort);
		self.coordinatorAddr = args[2];
		self.storageDir = args[1];
		self.startPingThread();
		ScheduledExecutorService getWorkerList = Executors.newSingleThreadScheduledExecutor();
		
		// get workers from coordinator
		getWorkerList.scheduleAtFixedRate(() -> {
			try {
				var resp = HTTP.doRequest("GET", "http://" + self.coordinatorAddr + "/workers", null);
				String body = new String(resp.body(), StandardCharsets.UTF_8);
				Map<String, String> latest = new HashMap<>();
				for (String line : body.split("\\r?\\n")) {
					line = line.trim();
					if (line.isEmpty())
						continue;
					String[] parts = line.split("\\s+");
					if (parts.length >= 2)
						latest.put(parts[0], parts[1]);
				}
				workersById.clear();
				workersById.putAll(latest);

				List<String> latestList = new ArrayList<>(latest.keySet());
				Collections.sort(latestList);
				workerList = Collections.unmodifiableList(latestList);
			} catch (Exception e) {
			}
		}, 0, 5, TimeUnit.SECONDS);

		// maintain the worker list
		ScheduledExecutorService maint = Executors.newSingleThreadScheduledExecutor();
		maint.scheduleAtFixedRate(() -> {
			try {
				List<String> curList = workerList;
				if (curList == null || curList.size() <= 1) {
					return;
				}
				int myIdx = curList.indexOf(self.workerId);
				if (myIdx < 0) {
					return;
				}
				int n = curList.size();

				// get the successors
				int[] successors;
				if (n == 2) {
					successors = new int[1];
					successors[0] = (myIdx + 1) % n;
				} else {
					successors = new int[2];
					successors[0] = (myIdx + 1) % n;
					successors[1] = (myIdx + 2) % n;
				}

				// check the successors
				for (int sIdx : successors) {
					String succId = curList.get(sIdx);
					String succAddr = workersById.get(succId);
					if (succAddr == null || succAddr.isBlank()) {
						continue;
					}

					String[] tables = getTable(succAddr);
					if (tables == null) {
						continue;
					}

					for (String table : tables) {
						if (table == null || table.isBlank()) {
							continue;
						}

						int nsIdx = (sIdx + 1) % n;
						String startRow = curList.get(sIdx);
						String endRowExclusive = curList.get(nsIdx);

						List<String[]> remote;
						if (startRow.compareTo(endRowExclusive) < 0) {
							remote = getHashKeys(succAddr, table, startRow, endRowExclusive);
							if (remote == null) {
								remote = Collections.emptyList();
							}
						} else {
							List<String[]> part1 = getHashKeys(succAddr, table, startRow, null);
							List<String[]> part2 = getHashKeys(succAddr, table, null, endRowExclusive);
							remote = new ArrayList<>();
							if (part1 != null) {
								remote.addAll(part1);
							}
							if (part2 != null) {
								remote.addAll(part2);
							}
						}

						for (String[] pair : remote) {
							if (pair == null || pair.length != 2) {
								continue;
							}
							String rowKey = pair[0];
							String remoteHash = pair[1];

							Row local = getRow(table, rowKey);
							String localHash;
							if (local == null) {
								localHash = null;
							} else {
								localHash = String.valueOf(Arrays.hashCode(local.toByteArray()));
							}

							if (local == null || !remoteHash.equals(localHash)) {
								updateRow(self, succAddr, table, rowKey);
							}
						}
					}
				}
			} catch (Exception e) {
			}
		}, 5, 30, TimeUnit.SECONDS);

		// get the root page
		get("/", (req, res) -> {
			SortedSet<String> names = new TreeSet<>(versions.keySet());

			File root = new File(self.storageDir);
			File[] kids = root.listFiles();
			if (kids != null) {
				for (File k : kids) {
					if (k.isDirectory()) {
						String nm = k.getName();
						if (nm.length() > 0) {
							names.add(nm);
						}
					}
				}
			}

			String html = 
					"<!doctype html>"
					+ "<html>"
						+ "<head>"
							+ "<title>Worker</title>"
						+ "</head>"
						+ "<body>";
			html += "<h1>Worker</h1>";
			html += "<table border=\"1\">";
			html += "<tr><th>Tables</th></tr>";
			if (names.isEmpty()) {
				html += "<tr><td>(none)</td></tr>";
			} else {
				for (String t : names) {
					html += "<tr><td>" + t + "</td></tr>";
				}
			}
			html += "</table>";
			html += "</body></html>";

			res.type("text/html");
			return html;
		});

		// put the data into the table
		put("/data/:table/:row/:col", (req, res) -> {
			String table = req.params("table");
			String rowKey = req.params("row");
			String col = req.params("col");
			byte[] value = req.bodyAsBytes();

			String ifcol = req.queryParams("ifcolumn");
			String equalsParam = req.queryParams("equals");
			boolean isReplica = req.queryParams("replica") != null;

			// synchronize on row-level to prevent race conditions
			// when multiple workers write different columns to the same row concurrently
			synchronized (getRowLock(table, rowKey)) {
				if (!isReplica && ifcol != null && equalsParam != null) {
					Row latest = getRow(table, rowKey);
					if (latest == null) {
						return "FAIL";
					}
					byte[] cur = latest.getBytes(ifcol);
					if (cur == null) {
						return "FAIL";
					}
					if (!Arrays.equals(cur, equalsParam.getBytes(StandardCharsets.UTF_8))) {
						return "FAIL";
					}
				}

				Row base = null;
				String cacheKey = null;

				if (table.startsWith("pt-")) {
					// cache for pt-tables
					cacheKey = getCacheKey(table, rowKey);
					base = ptTableCache.get(cacheKey);

					if (base == null) {
						// not in cache, try disk
						base = loadRowFromDisk(self.storageDir, table, rowKey);
						if (base != null) {
							base = base.clone();
						}
					} else {
						// clone from cache to avoid modifying cached version
						base = base.clone();
					}
				} else {
					base = getRow(table, rowKey);
				}

				if (base == null) {
					base = new Row(rowKey);
				}
				base.put(col, value);

				if (table.startsWith("pt-")) {
					// update cache with the modified row
					ptTableCache.put(cacheKey, base.clone());

					// write to disk
					File rowFile = getRowPath(self.storageDir, table, rowKey);
					File parent = rowFile.getParentFile();
					if (parent != null && !parent.exists()) {
						parent.mkdirs();
					}
					try (FileOutputStream fos = new FileOutputStream(rowFile, false)) {
						byte[] bytes = base.toByteArray();
						fos.write(bytes);
						fos.flush();
					} catch (Exception e) {

					}
					res.header("Version", "1");
				} else {
					int ver = putRow(table, rowKey, base);
					res.header("Version", String.valueOf(ver));
				}
			}

			if (!isReplica) {
				forwardToReplicas(self, table, rowKey, col, value);
			}
			return "OK";
		});

		// put the data into the table
		put("/data/:table", (req, res) -> {
			String table = req.params("table");
			byte[] body = req.bodyAsBytes();

			try {
				Row row = Row.readFrom(new java.io.ByteArrayInputStream(body));
				if (row == null) {
					res.status(400, "Bad Request");
					return "Invalid row data";
				}

				String rowKey = row.key();

				// synchronize on row-level to prevent race conditions
				synchronized (getRowLock(table, rowKey)) {
					if (table.startsWith("pt-")) {
						File rowFile = getRowPath(self.storageDir, table, rowKey);
						File parent = rowFile.getParentFile();
						if (parent != null && !parent.exists()) {
							parent.mkdirs();
						}
						try (FileOutputStream fos = new FileOutputStream(rowFile, false)) {
							byte[] bytes = row.toByteArray();
							fos.write(bytes);
							fos.flush();
						}
						res.header("Version", "1");
					} else {
						int ver = putRow(table, rowKey, row);
						res.header("Version", String.valueOf(ver));
					}
				}

				return "OK";
			} catch (Exception e) {
				res.status(500, "Internal Server Error");
				return "Error: " + e.getMessage();
			}
		});

		// get the data from the table
		get("/data/:table/:row/:col", (req, res) -> {
			String table = req.params("table");
			String rowKey = req.params("row");
			String col = req.params("col");

			Integer target = null;
			String ver = req.queryParams("version");
			if (ver != null) {
				try {
					target = Integer.valueOf(ver);
				} catch (NumberFormatException e) {
				}
			}

			Row row;
			if (target == null) {
				row = getRow(table, rowKey);
				if (row == null && table.startsWith("pt-")) {
					row = loadRowFromDisk(self.storageDir, table, rowKey);
				}
			} else {
				row = getRow(table, rowKey, target.intValue());
				if (row == null && table.startsWith("pt-")) {
					Row latest = loadRowFromDisk(self.storageDir, table, rowKey);
					if (latest != null) {
						row = getRow(table, rowKey, target.intValue());
					} else {
						row = null;
					}
				}
			}

			if (row == null) {
				res.status(404, "Not Found");
				return "Not Found";
			}

			byte[] val = row.getBytes(col);
			if (val == null) {
				res.status(404, "Not Found");
				return "Not Found";
			}

			if (table.startsWith("pt-")) {
				res.header("Version", "1");
			} else if (target != null) {
				res.header("Version", String.valueOf(target));
			} else {
				ConcurrentMap<String, NavigableMap<Integer, Row>> rowMap = versions.get(table);
				NavigableMap<Integer, Row> versionMap = null;
				if (rowMap != null) {
					versionMap = rowMap.get(rowKey);
				}
				int latest;
				if (versionMap == null || versionMap.isEmpty()) {
					latest = 0;
				} else {
					latest = versionMap.lastKey();
				}
				res.header("Version", String.valueOf(latest));
			}

			res.bodyAsBytes(val);
			return null;
		});

		// get the data from the table
		get("/data/:table", (req, res) -> {
			String table = req.params("table");
			String start = req.queryParams("startRow");
			String endEx = req.queryParams("endRowExclusive");

			boolean exists = versions.containsKey(table);
			File tableDir = new File(self.storageDir, table);
			if (!exists && (!tableDir.exists() || !tableDir.isDirectory())) {
				res.status(404, "Not Found");
				return "Not Found";
			}

			SortedSet<String> keys = new TreeSet<>();

			ConcurrentMap<String, NavigableMap<Integer, Row>> rowMap = versions.get(table);
			if (rowMap != null) {
				keys.addAll(rowMap.keySet());
			}

			if (tableDir.exists() && tableDir.isDirectory()) {
				Stack<File> stack = new Stack<>();
				Stack<String> prefixes = new Stack<>();
				stack.push(tableDir);
				prefixes.push("");

				while (!stack.isEmpty()) {
					File dir = stack.pop();
					String prefix = prefixes.pop();

					File[] kids = dir.listFiles();
					if (kids == null) {
						continue;
					}
					for (File f : kids) {
						if (f.isDirectory()) {
							if (f.getName().startsWith("__")) {
								stack.push(f);
								prefixes.push(prefix);
							} else {
								String decoded = KeyEncoder.decode(f.getName());
								stack.push(f);
								prefixes.push(prefix + decoded + "/");
							}
						} else {
							String decoded = KeyEncoder.decode(f.getName());
							keys.add(prefix + decoded);
						}
					}
				}
			}

			boolean hasRange = false;
			if (start != null) {
				hasRange = true;
			}
			if (endEx != null) {
				hasRange = true;
			}

			if (!hasRange) {
				res.type("text/plain");
				for (String k : keys) {
					Row r = getRow(table, k);
					if (r == null && table.startsWith("pt-")) {
						r = loadRowFromDisk(self.storageDir, table, k);
					}
					if (r != null) {
						byte[] enc = r.toByteArray();
						res.write(enc);
						res.write("\n".getBytes(StandardCharsets.UTF_8));
					}
				}
				res.write("\n".getBytes(StandardCharsets.UTF_8));
				return null;
			} else {
				res.type("application/octet-stream");
				for (String k : keys) {
					if (endEx != null) {
						if (k.compareTo(endEx) >= 0) {
							break;
						}
					}
					if (start != null) {
						if (k.compareTo(start) < 0) {
							continue;
						}
					}
					Row r = getRow(table, k);
					if (r == null && table.startsWith("pt-")) {
						r = loadRowFromDisk(self.storageDir, table, k);
					}
					if (r != null) {
						res.write(r.toByteArray());
						res.write("\n".getBytes(StandardCharsets.UTF_8));
					}
				}
				res.write("\n".getBytes(StandardCharsets.UTF_8));
				return null;
			}
		});

		// get the data from the table
		get("/data/:table/:row", (req, res) -> {
			String table = req.params("table");
			String rowKey = req.params("row");

			Row r = getRow(table, rowKey);
			if (r == null && table.startsWith("pt-")) {
				r = loadRowFromDisk(self.storageDir, table, rowKey);
			}
			if (r == null) {
				res.status(404, "Not Found");
				return "Not Found";
			}

			res.type("application/octet-stream");
			res.bodyAsBytes(r.toByteArray());
			return null;
		});

		// batch get multiple rows from the table
		post("/data/:table/batch", (req, res) -> {
			String table = req.params("table");
			byte[] body = req.bodyAsBytes();
			
			if (body == null || body.length == 0) {
				res.status(400, "Bad Request");
				return "Empty request body";
			}

			// parse comma-separated row keys
			String rowKeysStr = new String(body, java.nio.charset.StandardCharsets.UTF_8);
			String[] rowKeys = rowKeysStr.split(",");
			
			// fetch all rows and serialize response
			java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
			for (String rowKey : rowKeys) {
				rowKey = rowKey.trim();
				if (rowKey.isEmpty()) continue;
				
				try {
					Row r = getRow(table, rowKey);
					if (r == null && table.startsWith("pt-")) {
						r = loadRowFromDisk(self.storageDir, table, rowKey);
					}
					
					if (r != null) {
						byte[] keyBytes = rowKey.getBytes(java.nio.charset.StandardCharsets.UTF_8);
						byte[] rowBytes = r.toByteArray();
						
						int keyLen = keyBytes.length;
						baos.write((keyLen >> 24) & 0xFF);
						baos.write((keyLen >> 16) & 0xFF);
						baos.write((keyLen >> 8) & 0xFF);
						baos.write(keyLen & 0xFF);
						baos.write(keyBytes);
						
						int rowLen = rowBytes.length;
						baos.write((rowLen >> 24) & 0xFF);
						baos.write((rowLen >> 16) & 0xFF);
						baos.write((rowLen >> 8) & 0xFF);
						baos.write(rowLen & 0xFF);
						baos.write(rowBytes);
					}
				} catch (Exception e) {
				}
			}
			
			res.type("application/octet-stream");
			res.bodyAsBytes(baos.toByteArray());
			return null;
		});

		// count the number of rows in the table
		get("/count/:table", (req, res) -> {
			String table = req.params("table");

			boolean exists = versions.containsKey(table);
			File tableDir = new File(self.storageDir, table);
			if (!exists && (!tableDir.exists() || !tableDir.isDirectory())) {
				res.status(404, "Not Found");
				return "Not Found";
			}

			ConcurrentMap<String, NavigableMap<Integer, Row>> rowMap = versions.get(table);
			int count = 0;
			if (rowMap != null) {
				for (Entry<String, NavigableMap<Integer, Row>> rowKey : rowMap.entrySet()) {
					NavigableMap<Integer, Row> row = rowKey.getValue();
					if (row != null && !row.isEmpty()) {
						count++;
					}
				}
			}

			if (table.startsWith("pt-") && tableDir.exists() && tableDir.isDirectory()) {
				Stack<File> stack = new Stack<>();
				stack.push(tableDir);
				while (!stack.isEmpty()) {
					File dir = stack.pop();
					File[] kids = dir.listFiles();
					if (kids != null) {
						for (File f : kids) {
							if (f.isDirectory()) {
								stack.push(f);
							} else {
								count++;
							}
						}
					}
				}
			}

			res.type("text/plain");
			return String.valueOf(count);
		});

		// rename the table
		put("/rename/:old/", (req, res) -> {
			String oldName = req.params("old");
			String newName = new String(req.bodyAsBytes(), StandardCharsets.UTF_8);

			boolean oldExists = versions.containsKey(oldName);
			File oldDir = new File(self.storageDir, oldName);
			if (!oldExists && (!oldDir.exists() || !oldDir.isDirectory())) {
				res.status(404, "Not Found");
				return "Not Found";
			}

			boolean newExists = versions.containsKey(newName);
			File newDir = new File(self.storageDir, newName);
			if (newExists || (newDir.exists() && newDir.isDirectory())) {
				res.status(409, "Conflict");
				return "Conflict";
			}

			boolean oldPersistent = oldName.startsWith("pt-");
			boolean newPersistent = newName.startsWith("pt-");
			if (oldPersistent != newPersistent) {
				res.status(400, "Bad Request");
				return "Bad Request";
			}

			ConcurrentMap<String, NavigableMap<Integer, Row>> existing = versions.remove(oldName);
			if (existing != null) {
				versions.put(newName, existing);
			}

			if (oldDir.exists() && oldDir.isDirectory()) {
				boolean renamed = oldDir.renameTo(newDir);
				if (!renamed) {
					copyDir(oldDir, newDir);
					deleteDir(oldDir);
				}
			}

			return "OK";
		});

		// delete the table
		put("/delete/:table/", (req, res) -> {
			String table = req.params("table");

			boolean existed = false;

			ConcurrentMap<String, NavigableMap<Integer, Row>> removed = versions.remove(table);
			if (removed != null) {
				existed = true;
			}

			File dir = new File(self.storageDir, table);
			if (dir.exists() && dir.isDirectory()) {
				existed = true;
				deleteDir(dir);
			}

			if (!existed) {
				res.status(404, "Not Found");
				return "Not Found";
			}

			return "OK";
		});

		// get the tables
		get("/tables", (req, res) -> {
			SortedSet<String> names = new TreeSet<>(versions.keySet());

			File root = new File(self.storageDir);
			File[] kids = root.listFiles();
			if (kids != null) {
				for (File k : kids) {
					if (k.isDirectory()) {
						String nm = k.getName();
						if (!nm.equals(".") && !nm.equals("..")) {
							names.add(nm);
						}
					}
				}
			}
			String out = "";
			for (String n : names) {
				out += n + "\n";
			}
			res.type("text/plain");
			return out;
		});

		// get the view of the table
		get("/view/:table", (req, res) -> {
			String table = req.params("table");

			int page = 0;
			String p = req.queryParams("page");
			if (p != null) {
				try {
					page = Integer.parseInt(p);
				} catch (NumberFormatException e) {
				}
				if (page < 0) {
					page = 0;
				}
			}

		SortedSet<String> keys = new TreeSet<>();
		ConcurrentMap<String, NavigableMap<Integer, Row>> rowMap = versions.get(table);
		if (rowMap != null) {
			keys.addAll(rowMap.keySet());
		}

		// add pagination for persistent storage tables (e.g. pt-crawl)
		boolean exists = versions.containsKey(table);
		File tableDir = new File(self.storageDir, table);
		if (!exists && (!tableDir.exists() || !tableDir.isDirectory())) {
			res.status(404, "Not Found");
			return "Not Found";
		}
		if (tableDir.exists() && tableDir.isDirectory()) {
			Stack<File> stack = new Stack<>();
			Stack<String> prefixes = new Stack<>();
			stack.push(tableDir);
			prefixes.push("");

			while (!stack.isEmpty()) {
				File dir = stack.pop();
				String prefix = prefixes.pop();

				File[] kids = dir.listFiles();
				if (kids == null) {
					continue;
				}
				for (File f : kids) {
					if (f.isDirectory()) {
						if (f.getName().startsWith("__")) {
							stack.push(f);
							prefixes.push(prefix);
						} else {
							String decoded = KeyEncoder.decode(f.getName());
							stack.push(f);
							prefixes.push(prefix + decoded + "/");
						}
					} else {
						String decoded = KeyEncoder.decode(f.getName());
						keys.add(prefix + decoded);
					}
				}
			}
		}

		// calculate pagination after loading all the keys
		int pageSize = 10;
		int total = keys.size();
		int startIndex = page * pageSize;
		int endIndex = startIndex + pageSize;
		if (endIndex > total) {
			endIndex = total;
		}

			String html = "<!doctype html><html><head><title>View " + table + "</title></head><body>";
			html += "<h1>Table: " + table + "</h1>";
			html += "<table border=\"1\">";
			html += "<tr><th>Row Key</th><th>Columns</th></tr>";

			if (startIndex >= total) {
				html += "<tr><td colspan=\"2\"><em>No rows</em></td></tr>";
			} else {
				int i = 0;
				for (String k : keys) {
					if (i >= startIndex && i < endIndex) {
						Row r = getRow(table, k);
						if (r == null && table.startsWith("pt-")) {
							r = loadRowFromDisk(self.storageDir, table, k);
						}
						html += "<tr>";
						html += "<td>" + k + "</td>";
						html += "<td>";
					if (r != null) {
						for (String col : r.columns()) {
							String v = r.get(col);
							if (v == null)
								v = "";
							// escape the HTML text to prevent rendering crawled page in table
							html += "<div>" + col + " = " + escapeHtml(v) + "</div>";
						}
					}
						html += "</td>";
						html += "</tr>";
					}
					i++;
					if (i >= endIndex)
						break;
				}
			}

		html += "</table>";

		// Pagination controls (e.g. pages x of y....)
		html += "<p>";
		if (page > 0) {
			html += "<a href=\"/view/" + table + "?page=" + (page - 1) + "\">Previous</a> ";
		}
		html += "Page " + (page + 1) + " of " + ((total + pageSize - 1) / pageSize);
		if (endIndex < total) {
			html += " <a href=\"/view/" + table + "?page=" + (page + 1) + "\">Next</a>";
		}
		html += "</p>";

		html += "</body></html>";

			res.type("text/html");
			return html;
		});

		// get the hashes of the table
		get("/hashes/:table", (req, res) -> {
			String table = req.params("table");
			String start = req.queryParams("startRow");
			String end = req.queryParams("endRowExclusive");
			res.type("text/plain");

			SortedSet<String> keys = new TreeSet<>();

			ConcurrentMap<String, NavigableMap<Integer, Row>> rowMap = versions.get(table);
			if (rowMap != null) {
				keys.addAll(rowMap.keySet());
			}

			File tableDir = new File(self.storageDir, table);
			if (tableDir.exists() && tableDir.isDirectory()) {
				Stack<File> stack = new Stack<>();
				Stack<String> prefixes = new Stack<>();
				stack.push(tableDir);
				prefixes.push("");
				while (!stack.isEmpty()) {
					File dir = stack.pop();
					String prefix = prefixes.pop();
					File[] kids = dir.listFiles();
					if (kids == null) {
						continue;
					}
					for (File f : kids) {
						String decoded = KeyEncoder.decode(f.getName());
						if (f.isDirectory()) {
							if (f.getName().startsWith("__")) {
								stack.push(f);
								prefixes.push(prefix);
							} else {
								stack.push(f);
								prefixes.push(prefix + decoded + "/");
							}
						} else {
							keys.add(prefix + decoded);
						}
					}
				}
			}

			for (String k : keys) {
				if (end != null && k.compareTo(end) >= 0) {
					break;
				}
				if (start != null && k.compareTo(start) < 0) {
					continue;
				}

				Row r = getRow(table, k);
				if (r == null && table.startsWith("pt-"))
					r = loadRowFromDisk(self.storageDir, table, k);
				if (r != null) {
					int hash = Arrays.hashCode(r.toByteArray());
					String line = k + " " + hash + "\n";
					res.write(line.getBytes(StandardCharsets.UTF_8));
				}
			}
			return null;
		});

	}

	// get the worker ID
	private static String getWorkerId(String storageDir, int httpPort) {
		File dir = new File(storageDir);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		File f = new File(dir, "id-" + httpPort);

		String id = null;

		if (f.exists() && f.isFile()) {
			try (BufferedReader br = new BufferedReader(new FileReader(f))) {
				String line = br.readLine();
				if (line != null) {
					line = line.trim();
					if (!line.isEmpty()) {
						id = line;
					}
				}
			} catch (Exception e) {

			}
		}
		if (id == null) {
			id = cis5550.generic.Worker.randomId5();
			try (BufferedWriter bw = new BufferedWriter(new FileWriter(f, false))) {
				bw.write(id);
				bw.newLine();
			} catch (Exception e) {

			}
		}
		return id;
	}

	// get the row from the table
	private static Row getRow(String table, String rowKey) {
		ConcurrentMap<String, NavigableMap<Integer, Row>> rowMap = versions.get(table);
		if (rowMap == null) {
			return null;
		}

		NavigableMap<Integer, Row> versionMap = rowMap.get(rowKey);
		if (versionMap == null || versionMap.isEmpty()) {
			return null;
		}

		return versionMap.lastEntry().getValue().clone();
	}

	// get the row from the table
	private static Row getRow(String table, String rowKey, int version) {
		ConcurrentMap<String, NavigableMap<Integer, Row>> rowMap = versions.get(table);
		if (rowMap == null) {
			return null;
		}
		NavigableMap<Integer, Row> versionMap = rowMap.get(rowKey);
		if (versionMap == null) {
			return null;
		}
		Row snap = versionMap.get(version);

		if (snap == null) {
			return null;
		} else {
			return snap.clone();
		}
	}

	// put the row into the table
	private static int putRow(String table, String rowKey, Row newState) {
		ConcurrentMap<String, NavigableMap<Integer, Row>> rowMap = versions.computeIfAbsent(table,
				t -> new ConcurrentHashMap<>());

		NavigableMap<Integer, Row> versionMap = rowMap.computeIfAbsent(rowKey, rk -> new ConcurrentSkipListMap<>());

		synchronized (versionMap) {
			int next;
			if (versionMap.isEmpty()) {
				next = 1;
			} else {
				next = versionMap.lastKey() + 1;
			}
			versionMap.put(next, newState.clone());
			return next;
		}
	}

	// delete the directory
	private static void deleteDir(File f) {
		if (f.isDirectory()) {
			File[] kids = f.listFiles();
			if (kids != null) {
				for (File k : kids) {
					deleteDir(k);
				}
			}
		}
		try {
			f.delete();
		} catch (Exception e) {

		}
	}

	// copy the directory
	private static void copyDir(File src, File dst) throws IOException {
		if (!dst.exists()) {
			dst.mkdirs();
		}
		File[] kids = src.listFiles();
		if (kids == null) {
			return;
		}
		for (File k : kids) {
			File target = new File(dst, k.getName());
			if (k.isDirectory()) {
				copyDir(k, target);
			} else {
				try (FileInputStream in = new FileInputStream(k); FileOutputStream out = new FileOutputStream(target)) {
					byte[] buf = new byte[10000];
					int n;
					while ((n = in.read(buf)) > 0) {
						out.write(buf, 0, n);
					}
				}
			}
		}
	}

	// load the row from the disk
	private static Row loadRowFromDisk(String storageDir, String table, String rowKey) {
		File rowFile = getRowPath(storageDir, table, rowKey);
		if (!rowFile.exists() || !rowFile.isFile())
			return null;
		try (FileInputStream fis = new FileInputStream(rowFile)) {
			Row r = Row.readFrom(fis);
			if (!table.startsWith("pt-")) {
				putRow(table, rowKey, r);
			}
			return r;
		} catch (Exception e) {
			return null;
		}
	}

	// get the row path
	private static File getRowPath(String storageDir, String table, String rowKey) {
		File tableDir = new File(storageDir, table);
		String encoded = KeyEncoder.encode(rowKey);

		if (encoded.length() >= 6) {
			String subdir = "__" + encoded.substring(0, 2);
			return new File(new File(tableDir, subdir), encoded);
		} else {
			return new File(tableDir, encoded);
		}
	}

	// forward the data to the replicas
	private static void forwardToReplicas(Worker self, String table, String rowKey, String col, byte[] value) {
		List<String> curList = workerList;
		if (curList.isEmpty() || curList.size() == 1) {
			return;
		}

		int idx = curList.indexOf(self.workerId);
		if (idx < 0) {
			return;
		}

		int n = curList.size();

		int idx1 = (idx - 1 + n) % n;
		int idx2 = (idx - 2 + n) % n;

		Set<Integer> targets = new LinkedHashSet<>();
		if (idx1 != idx) {
			targets.add(idx1);
		}
		if (idx2 != idx) {
			targets.add(idx2);
		}

		for (int tIdx : targets) {
			String tid = curList.get(tIdx);
			String addr = workersById.get(tid);
			if (addr == null)
				continue;

			String url = "http://" + addr + "/data/" + table + "/" + rowKey + "/" + col + "?replica=1";
			try {
				HTTP.doRequest("PUT", url, value);
			} catch (Exception e) {
			}
		}
	}

	// get the tables
	private static String[] getTable(String workerAddr) {
		try {
			String url = "http://" + workerAddr + "/tables";
			byte[] body = HTTP.doRequest("GET", url, null).body();
			String txt = new String(body, StandardCharsets.UTF_8);
			if (txt.isBlank()) {
				return new String[0];
			} else {
				return txt.split("\\r?\\n");
			}
		} catch (Exception e) {
			return null;
		}
	}

	// get the hash keys
	private static List<String[]> getHashKeys(String workerAddr, String table, String start, String end) {
		try {
			StringBuilder url = new StringBuilder("http://" + workerAddr + "/hashes/" + table);
			boolean hasParam = false;
			if (start != null) {
				url.append(hasParam ? "&" : "?");
				url.append("startRow=").append(URLEncoder.encode(start, StandardCharsets.UTF_8));
				hasParam = true;
			}
			if (end != null) {
				url.append(hasParam ? "&" : "?");
				url.append("endRowExclusive=").append(URLEncoder.encode(end, StandardCharsets.UTF_8));
			}

			byte[] body = HTTP.doRequest("GET", url.toString(), null).body();
			String[] lines = new String(body, StandardCharsets.UTF_8).split("\\r?\\n");
			List<String[]> out = new ArrayList<>();
			for (String line : lines) {
				if (line == null || line.isBlank()) {
					continue;
				}
				int sp = line.lastIndexOf(' ');
				if (sp <= 0) {
					continue;
				}
				String k = line.substring(0, sp);
				String h = line.substring(sp + 1);
				out.add(new String[] { k, h });
			}
			return out;
		} catch (Exception e) {
			return null;
		}
	}

	// update the row
	private static void updateRow(Worker self, String sourceAddr, String table, String rowKey) {
		try {
			String rowUrl = "http://" + sourceAddr + "/data/" + table + "/" + rowKey;
			byte[] rb = HTTP.doRequest("GET", rowUrl, null).body();
			try (ByteArrayInputStream bin = new ByteArrayInputStream(rb)) {
				Row fetched = Row.readFrom(bin);
				if (fetched == null)
					return;

				if (table.startsWith("pt-")) {
					File f = getRowPath(self.storageDir, table, rowKey);
					File parent = f.getParentFile();
					if (parent != null)
						parent.mkdirs();
					try (FileOutputStream fos = new FileOutputStream(f, false)) {
						fos.write(fetched.toByteArray());
					}
				} else {
					putRow(table, rowKey, fetched);
				}
			}
		} catch (Exception e) {
		}
	}

	/**
	 * a helper method to escape HTML links in table to prevent rending
	 * @param text
	 * @return escaped replacement text
	 * @author Evan Law
	 */
	private static String escapeHtml(String text) {
		if (text == null) return "";
		return text.replace("&", "&amp;")
				   .replace("<", "&lt;")
				   .replace(">", "&gt;")
				   .replace("\"", "&quot;")
				   .replace("'", "&#x27;");
	}
}