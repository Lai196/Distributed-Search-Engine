package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

class FlamePairRDDImpl implements FlamePairRDD {
	private final KVSClient kvs;
	private final String table;
	private volatile boolean destroyed = false;

	private void checkAlive() {
		if (destroyed) {
			throw new IllegalStateException("PairRDD destroyed");
		}
	}

	FlamePairRDDImpl(KVSClient kvs, String table) {
		this.kvs = kvs;
		this.table = table;
	}

	String getTableName() {
		return table;
	}

	KVSClient getKVS() {
		return kvs;
	}

	@Override
	public List<FlamePair> collect() throws Exception {
		checkAlive();
		ArrayList<FlamePair> out = new ArrayList<>();
		Iterator<Row> it = kvs.scan(table);
		while (it.hasNext()) {
			Row r = it.next();
			String k = r.key();
			Set<String> cols = r.columns();
			if (cols != null) {
				for (String c : cols) {
					String v = r.get(c);
					if (v != null)
						out.add(new FlamePair(k, v));
				}
			}
		}
		return out;
	}

	@Override
	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
		checkAlive();
		byte[] f = Serializer.objectToByteArray(lambda);
		String zero;
		if (zeroElement == null) {
			zero = "";
		} else {
			zero = zeroElement;
		}
		String extra = "zero=" + URLEncoder.encode(zero, "UTF-8");
		String outTable = Coordinator.startRDD("/pair/foldByKey", table, f, extra);
		return new FlamePairRDDImpl(kvs, outTable);
	}

	@Override
	public FlamePairRDD foldByKey(String zeroElement, StringAccumulator lambda) throws Exception {
		checkAlive();
		byte[] f = Serializer.objectToByteArray(lambda);
		String zero;
		if (zeroElement == null) {
			zero = "";
		} else {
			zero = zeroElement;
		}
		String extra = "zero=" + URLEncoder.encode(zero, "UTF-8");
		String outTable = Coordinator.startRDD("/pair/foldByKey", table, f, extra);
		return new FlamePairRDDImpl(kvs, outTable);
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		checkAlive();
		var it = kvs.scan(table);
		while (it.hasNext()) {
			var row = it.next();
			var key = row.key();
			var cols = row.columns();
			if (cols == null || cols.isEmpty()) {
				continue;
			}
			// use putRow() to write the row at once instead of individual put()
			Row newRow = new Row(key);
			for (String c : cols) {
				var val = row.get(c);
				if (val != null) {
					newRow.put(c, val);
				}
			}
			kvs.putRow(tableNameArg, newRow);
		}
	}

	@Override
	public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
		checkAlive();
		byte[] f = Serializer.objectToByteArray(lambda);
		String outTable = Coordinator.startRDD("/pair/flatMap", table, f);
		return new FlameRDDImpl(kvs, outTable);
	}

	@Override
	public void destroy() throws Exception {
		if (destroyed) {
			return;
		}
		kvs.delete(table);
		destroyed = true;
	}

	@Override
	public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
		checkAlive();
		byte[] f = Serializer.objectToByteArray(lambda);
		String outTable = Coordinator.startRDD("/pair/flatMapToPair", table, f);
		return new FlamePairRDDImpl(kvs, outTable);
	}

	@Override
	public FlamePairRDD join(FlamePairRDD other) throws Exception {
		checkAlive();
		String otherTable = ((FlamePairRDDImpl) other).getTableName();
		String extra = "other=" + URLEncoder.encode(otherTable, "UTF-8");
		String outTable = Coordinator.startRDD("/pair/join", table, null, extra);
		return new FlamePairRDDImpl(kvs, outTable);
	}

	@Override
	public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
		checkAlive();
		String otherTable = ((FlamePairRDDImpl) other).getTableName();
		String extra = "other=" + URLEncoder.encode(otherTable, "UTF-8");
		String outTable = Coordinator.startRDD("/pair/cogroup", this.table, null, extra);
		return new FlamePairRDDImpl(kvs, outTable);
	}
}
