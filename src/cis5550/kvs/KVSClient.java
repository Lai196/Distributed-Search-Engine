package cis5550.kvs;

import java.util.*;
import java.util.concurrent.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.io.*;
import cis5550.tools.HTTP;

public class KVSClient implements KVS {
	
	String coordinator;

	static class WorkerEntry implements Comparable<WorkerEntry> {
		String address;
		String id;

		WorkerEntry(String addressArg, String idArg) {
			address = addressArg;
			id = idArg;
		}

		public int compareTo(WorkerEntry e) {
			return id.compareTo(e.id);
		}
	};

	Vector<WorkerEntry> workers;
	boolean haveWorkers;

	public int numWorkers() throws IOException {
		if (!haveWorkers)
			downloadWorkers();
		return workers.size();
	}

	public static String getVersion() {
		return "v1.5 Oct 20 2023";
	}

	public String getCoordinator() {
		return coordinator;
	}

	public String getWorkerAddress(int idx) throws IOException {
		if (!haveWorkers)
			downloadWorkers();
		return workers.elementAt(idx).address;
	}

	public String getWorkerID(int idx) throws IOException {
		if (!haveWorkers)
			downloadWorkers();
		return workers.elementAt(idx).id;
	}

	// KVSIterator class to iterate over the rows in the table
	class KVSIterator implements Iterator<Row> {
		InputStream in;
		boolean atEnd;
		Row nextRow;
		int currentRangeIndex;
		String endRowExclusive;
		String startRow;
		String tableName;
		Vector<String> ranges;

		// Constructor
		KVSIterator(String tableNameArg, String startRowArg, String endRowExclusiveArg) throws IOException {
			in = null;
			currentRangeIndex = 0;
			atEnd = false;
			endRowExclusive = endRowExclusiveArg;
			tableName = tableNameArg;
			startRow = startRowArg;
			ranges = new Vector<String>();
			
			// if the start row is null or less than the first worker ID, add the URL for the last worker
			if ((startRowArg == null) || (startRowArg.compareTo(getWorkerID(0)) < 0)) {
				String url = getURL(tableNameArg, numWorkers() - 1, startRowArg,
						((endRowExclusiveArg != null) && (endRowExclusiveArg.compareTo(getWorkerID(0)) < 0))
								? endRowExclusiveArg
								: getWorkerID(0));
				ranges.add(url);
			}
			// for each worker, add the URL for the rows in the table
			for (int i = 0; i < numWorkers(); i++) {
				if ((startRowArg == null) || (i == numWorkers() - 1)
						|| (startRowArg.compareTo(getWorkerID(i + 1)) < 0)) {
					if ((endRowExclusiveArg == null) || (endRowExclusiveArg.compareTo(getWorkerID(i)) > 0)) {
						boolean useActualStartRow = (startRowArg != null)
								&& (startRowArg.compareTo(getWorkerID(i)) > 0);
						boolean useActualEndRow = (endRowExclusiveArg != null) && ((i == (numWorkers() - 1))
								|| (endRowExclusiveArg.compareTo(getWorkerID(i + 1)) < 0));
						String url = getURL(tableNameArg, i, useActualStartRow ? startRowArg : getWorkerID(i),
								useActualEndRow ? endRowExclusiveArg
										: ((i < numWorkers() - 1) ? getWorkerID(i + 1) : null));
						ranges.add(url);
					}
				}
			}
			openConnectionAndFill();
		}

		// get the URL for the rows in the table
		protected String getURL(String tableNameArg, int workerIndexArg, String startRowArg, String endRowExclusiveArg)
				throws IOException {
			String params = "";
			if (startRowArg != null)
				params = "startRow=" + startRowArg;
			if (endRowExclusiveArg != null)
				params = (params.equals("") ? "" : (params + "&")) + "endRowExclusive=" + endRowExclusiveArg;
			return "http://" + getWorkerAddress(workerIndexArg) + "/data/" + tableNameArg
					+ (params.equals("") ? "" : "?" + params);
		}

		// open the connection and fill the rows in the table
		void openConnectionAndFill() {
			try {
				if (in != null) {
					in.close();
					in = null;
				}

				if (atEnd)
					return;

				while (true) {
					if (currentRangeIndex >= ranges.size()) {
						atEnd = true;
						return;
					}

					try {
						URL url = new URI(ranges.elementAt(currentRangeIndex)).toURL();
						HttpURLConnection con = (HttpURLConnection) url.openConnection();
						con.setRequestMethod("GET");
						con.connect();
						in = con.getInputStream();
						Row r = fill();
						if (r != null) {
							nextRow = r;
							break;
						}
					} catch (FileNotFoundException fnfe) {
					} catch (URISyntaxException use) {
					}

					currentRangeIndex++;
				}
			} catch (IOException ioe) {
				if (in != null) {
					try {
						in.close();
					} catch (Exception e) {
					}
					in = null;
				}
				atEnd = true;
			}
		}

		// fill the rows in the table
		synchronized Row fill() {
			try {
				Row r = Row.readFrom(in);
				return r;
			} catch (Exception e) {
				return null;
			}
		}

		// get the next row in the table
		public synchronized Row next() {
			if (atEnd)
				return null;
			Row r = nextRow;
			nextRow = fill();
			while ((nextRow == null) && !atEnd) {
				currentRangeIndex++;
				openConnectionAndFill();
			}

			return r;
		}

		// check if there is a next row in the table
		public synchronized boolean hasNext() {
			return !atEnd;
		}
	}

	// download the workers from the coordinator
	synchronized void downloadWorkers() throws IOException {
		String result = new String(HTTP.doRequest("GET", "http://" + coordinator + "/workers", null).body());
		String[] pieces = result.split("\n");
		int numWorkers = Integer.parseInt(pieces[0]);
		if (numWorkers < 1)
			throw new IOException("No active KVS workers");
		if (pieces.length != (numWorkers + 1))
			throw new RuntimeException("Received truncated response when asking KVS coordinator for list of workers");
		workers.clear();
		for (int i = 0; i < numWorkers; i++) {
			String[] pcs = pieces[1 + i].split(",");
			workers.add(new WorkerEntry(pcs[1], pcs[0]));
		}
		Collections.sort(workers);

		haveWorkers = true;
	}

	// get the index of the worker for the given key
	int workerIndexForKey(String key) {
		int chosenWorker = workers.size() - 1;
		if (key != null) {
			for (int i = 0; i < workers.size() - 1; i++) {
				if ((key.compareTo(workers.elementAt(i).id) >= 0) && (key.compareTo(workers.elementAt(i + 1).id) < 0))
					chosenWorker = i;
			}
		}

		return chosenWorker;
	}

	public KVSClient(String coordinatorArg) {
		coordinator = coordinatorArg;
		workers = new Vector<WorkerEntry>();
		haveWorkers = false;
	}

	// rename the table
	public boolean rename(String oldTableName, String newTableName) throws IOException {
		if (!haveWorkers)
			downloadWorkers();

		boolean result = true;
		for (WorkerEntry w : workers) {
			try {
				byte[] response = HTTP.doRequest("PUT",
						"http://" + w.address + "/rename/" + URLEncoder.encode(oldTableName, "UTF-8") + "/",
						newTableName.getBytes()).body();
				if (response != null) {
					String res = new String(response);
					result &= res.equals("OK");
				}
			} catch (Exception e) {
				result = false;
			}
		}
		return result;
	}

	// delete the table
	public void delete(String oldTableName) throws IOException {
		if (!haveWorkers)
			downloadWorkers();

		for (WorkerEntry w : workers) {
			try {
				HTTP.doRequest("PUT",
						"http://" + w.address + "/delete/" + URLEncoder.encode(oldTableName, "UTF-8") + "/",
						null);
			} catch (Exception e) {
			}
		}
	}

	// put the value into the table
	public void put(String tableName, String row, String column, byte value[]) throws IOException {
		if (!haveWorkers)
			downloadWorkers();

		try {
			String target = "http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/" + tableName + "/"
					+ URLEncoder.encode(row, "UTF-8") + "/" + URLEncoder.encode(column, "UTF-8");
			byte[] response = HTTP.doRequest("PUT", target, value).body();
			String result = new String(response);
			if (!result.equals("OK"))
				throw new RuntimeException("PUT returned something other than OK: " + result + "(" + target + ")");
		} catch (UnsupportedEncodingException uee) {
			throw new RuntimeException("UTF-8 encoding not supported?!?");
		}
	}

	// put the value into the table
	public void put(String tableName, String row, String column, String value) throws IOException {
		put(tableName, row, column, value.getBytes());
	}

	// put the row into the table
	public void putRow(String tableName, Row row) throws FileNotFoundException, IOException {
		if (!haveWorkers)
			downloadWorkers();
		if (row.key().equals(""))
			throw new RuntimeException("Row key can't be empty!");

		byte[] response = HTTP.doRequest("PUT", "http://"+workers.elementAt(workerIndexForKey(row.key())).address+"/data/"+tableName, row.toByteArray()).body();
		String result = new String(response);
		if (!result.equals("OK"))
			throw new RuntimeException("PUT returned something other than OK: " + result);
	}

	// get the row from the table
	public Row getRow(String tableName, String row) throws IOException {
		if (!haveWorkers)
			downloadWorkers();
		if (row.equals(""))
			throw new RuntimeException("Row key can't be empty!");

		HTTP.Response resp = HTTP.doRequest("GET", "http://"+workers.elementAt(workerIndexForKey(row)).address+"/data/"+tableName+"/"+URLEncoder.encode(row, "UTF-8"), null);
		if (resp.statusCode() == 404)
			return null;

		byte[] result = resp.body();
		if (result == null || result.length == 0)
			return null;

		try {
			return Row.readFrom(new ByteArrayInputStream(result));
		} catch (Exception e) {
			throw new RuntimeException("Decoding error while reading Row '" + row + "' in table '" + tableName
					+ "' from getRow() URL (encoded as '" + URLEncoder.encode(row, "UTF-8")
					+ "'). Actual error: " + e.getMessage(), e);
		}
	}

	// get multiple rows from the table in batch
	public Map<String, Row> getRows(String tableName, List<String> rows) throws IOException {
		if (!haveWorkers)
			downloadWorkers();
		if (rows == null || rows.isEmpty())
			return new HashMap<>();

		// group rows by worker to minimize requests
		Map<Integer, List<String>> rowsByWorker = new HashMap<>();
		for (String row : rows) {
			if (row == null || row.equals(""))
				continue;
			int workerIdx = workerIndexForKey(row);
			rowsByWorker.computeIfAbsent(workerIdx, k -> new ArrayList<>()).add(row);
		}

		// fetch from each worker in parallel
		Map<String, Row> results = new ConcurrentHashMap<>();
		ExecutorService pool = Executors.newFixedThreadPool(Math.min(rowsByWorker.size(), 20));
		List<Future<Void>> futures = new ArrayList<>();

		for (Map.Entry<Integer, List<String>> entry : rowsByWorker.entrySet()) {
			int workerIdx = entry.getKey();
			List<String> workerRows = entry.getValue();
			String workerAddr = workers.elementAt(workerIdx).address;

			futures.add(pool.submit(() -> {
				try {
					// send row keys as comma-separated in POST body
					String rowKeys = String.join(",", workerRows);
					String url = "http://" + workerAddr + "/data/" + tableName + "/batch";
					HTTP.Response resp = HTTP.doRequest("POST", url, rowKeys.getBytes(StandardCharsets.UTF_8));

					if (resp.statusCode() == 200) {
						byte[] body = resp.body();
						if (body != null && body.length > 0) {
							ByteArrayInputStream bis = new ByteArrayInputStream(body);
							while (bis.available() >= 4) {
								try {
									byte[] keyLenBytes = new byte[4];
									int keyLenRead = bis.read(keyLenBytes);
									if (keyLenRead != 4) break;
									int keyLen = ((keyLenBytes[0] & 0xFF) << 24) | 
												((keyLenBytes[1] & 0xFF) << 16) | 
												((keyLenBytes[2] & 0xFF) << 8) | 
												(keyLenBytes[3] & 0xFF);
									
									if (bis.available() < keyLen + 4) break;
									
									byte[] keyBytes = new byte[keyLen];
									int keyRead = bis.read(keyBytes);
									if (keyRead != keyLen) break;
									String key = new String(keyBytes, StandardCharsets.UTF_8);
									
									byte[] rowLenBytes = new byte[4];
									int rowLenRead = bis.read(rowLenBytes);
									if (rowLenRead != 4) break;
									int rowLen = ((rowLenBytes[0] & 0xFF) << 24) | 
												((rowLenBytes[1] & 0xFF) << 16) | 
												((rowLenBytes[2] & 0xFF) << 8) | 
												(rowLenBytes[3] & 0xFF);
									
									if (bis.available() < rowLen) break;
									
									byte[] rowBytes = new byte[rowLen];
									int rowRead = bis.read(rowBytes);
									if (rowRead != rowLen) break;
									
									Row row = Row.readFrom(new ByteArrayInputStream(rowBytes));
									if (row != null) {
										results.put(key, row);
									}
								} catch (Exception e) {
									break;
								}
							}
						}
					}
				} catch (Exception e) {
					for (String row : workerRows) {
						try {
							Row r = getRow(tableName, row);
							if (r != null) {
								results.put(row, r);
							}
						} catch (Exception ex) {
						}
					}
				}
				return null;
			}));
		}

		for (Future<Void> future : futures) {
			try {
				future.get();
			} catch (Exception e) {
			}
		}

		pool.shutdown();
		try {
			pool.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}

		return results;
	}

	// get the value from the table
	public byte[] get(String tableName, String row, String column) throws IOException {
		if (!haveWorkers)
			downloadWorkers();
		if (row.equals(""))
			throw new RuntimeException("Row key can't be empty!");

		HTTP.Response res = HTTP.doRequest("GET", "http://"+workers.elementAt(workerIndexForKey(row)).address+"/data/"+tableName+"/"+URLEncoder.encode(row, "UTF-8")+"/"+URLEncoder.encode(column, "UTF-8"), null);
		return ((res != null) && (res.statusCode() == 200)) ? res.body() : null;
	}

	// check if the row exists in the table
	public boolean existsRow(String tableName, String row) throws FileNotFoundException, IOException {
		if (!haveWorkers)
			downloadWorkers();

		HTTP.Response r = HTTP.doRequest("GET", "http://"+workers.elementAt(workerIndexForKey(row)).address+"/data/"+tableName+"/"+URLEncoder.encode(row, "UTF-8"), null);
		return r.statusCode() == 200;
	}

	// count the number of rows in the table
	public int count(String tableName) throws IOException {
		if (!haveWorkers)
			downloadWorkers();

		int total = 0;
		for (WorkerEntry w : workers) {
			try {
				HTTP.Response r = HTTP.doRequest("GET", "http://" + w.address + "/count/" + tableName, null);
				if ((r != null) && (r.statusCode() == 200)) {
					String result = new String(r.body());
					total += Integer.valueOf(result).intValue();
				}
			} catch (IOException e) {
			}
		}
		return total;
	}

	// scan the table
	public Iterator<Row> scan(String tableName) throws FileNotFoundException, IOException {
		return scan(tableName, null, null);
	}

	// scan the table
	public Iterator<Row> scan(String tableName, String startRow, String endRowExclusive)
			throws FileNotFoundException, IOException {
		if (!haveWorkers)
			downloadWorkers();

		return new KVSIterator(tableName, startRow, endRowExclusive);
	}

	public static void main(String args[]) throws Exception {
		if (args.length < 2) {
			System.err.println("Syntax: client <coordinator> get <tableName> <row> <column>");
			System.err.println("Syntax: client <coordinator> put <tableName> <row> <column> <value>");
			System.err.println("Syntax: client <coordinator> scan <tableName>");
			System.err.println("Syntax: client <coordinator> count <tableName>");
			System.err.println("Syntax: client <coordinator> rename <oldTableName> <newTableName>");
			System.err.println("Syntax: client <coordinator> delete <tableName>");
			System.exit(1);
		}

		KVSClient client = new KVSClient(args[0]);

		if (args[1].equals("put")) {
			if (args.length != 6) {
				System.err.println("Syntax: client <coordinator> put <tableName> <row> <column> <value>");
				System.exit(1);
			}
			client.put(args[2], args[3], args[4], args[5].getBytes("UTF-8"));
		} else if (args[1].equals("get")) {
			if (args.length != 5) {
				System.err.println("Syntax: client <coordinator> get <tableName> <row> <column>");
				System.exit(1);
			}
			byte[] val = client.get(args[2], args[3], args[4]);
			if (val == null)
				System.err.println("No value found");
			else
				System.out.write(val);
		} else if (args[1].equals("scan")) {
			if (args.length != 3) {
				System.err.println("Syntax: client <coordinator> scan <tableName>");
				System.exit(1);
			}

			Iterator<Row> iter = client.scan(args[2], null, null);
			int count = 0;
			while (iter.hasNext()) {
				System.out.println(iter.next());
				count++;
			}
			System.err.println(count + " row(s) scanned");
		} else if (args[1].equals("count")) {
			if (args.length != 3) {
				System.err.println("Syntax: client <coordinator> count <tableName>");
				System.exit(1);
			}

			System.out.println(client.count(args[2]) + " row(s) in table '" + args[2] + "'");
		} else if (args[1].equals("delete")) {
			if (args.length != 3) {
				System.err.println("Syntax: client <coordinator> delete <tableName>");
				System.exit(1);
			}

			client.delete(args[2]);
			System.err.println("Table '" + args[2] + "' deleted");
		} else if (args[1].equals("rename")) {
			if (args.length != 4) {
				System.err.println("Syntax: client <coordinator> rename <oldTableName> <newTableName>");
				System.exit(1);
			}
			if (client.rename(args[2], args[3]))
				System.out.println("Success");
			else
				System.out.println("Failure");
		} else {
			System.err.println("Unknown command: " + args[1]);
			System.exit(1);
		}
	}


	public List<String> listTables() throws java.io.IOException {

		if (!haveWorkers) downloadWorkers();

		if (workers.isEmpty()) {
			throw new IOException("No active KVS Workers!");
		}

		WorkerEntry we = workers.elementAt(0);

		byte[] body = HTTP.doRequest("GET", "http://" + we.address + "/tables", null).body();
		String text = new String(body, StandardCharsets.UTF_8);
		String[] lines = text.split("\\r?\\n");

		java.util.List<String> result = new java.util.ArrayList<>();
		for (String line : lines) {
			String t = line.trim();
			if (!t.isEmpty()) {
				result.add(t);
			}
		}

		java.util.Collections.sort(result);
		return result;
	}
};