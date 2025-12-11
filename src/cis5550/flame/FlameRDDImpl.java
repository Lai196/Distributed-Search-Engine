package cis5550.flame;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;
import java.util.*;
import java.net.URLEncoder;

class FlameRDDImpl implements FlameRDD {
	private final KVSClient kvs;
	private final String table;
	private volatile boolean destroyed = false;

	private void checkAlive() {
		if (destroyed) {
			throw new IllegalStateException("RDD destroyed");
		}
	}

	FlameRDDImpl(KVSClient kvs, String table) {
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
	public FlameRDD flatMap(StringToIterable f) throws Exception {
		checkAlive();
		byte[] lambda = Serializer.objectToByteArray(f);
		String outTable = Coordinator.startRDD("/rdd/flatMap", table, lambda);
		return new FlameRDDImpl(kvs, outTable);
	}

	@Override
	public List<String> collect() throws Exception {
		checkAlive();
		ArrayList<String> out = new ArrayList<>();
		Iterator<Row> it = kvs.scan(table);
		while (it.hasNext()) {
			Row r = it.next();
			String val = r.get("value");
			if (val != null)
				out.add(val);
		}
		return out;
	}

	@Override
	public FlamePairRDD mapToPair(StringToPair f) throws Exception {
		checkAlive();
		byte[] lambda = Serializer.objectToByteArray(f);
		String outTable = Coordinator.startRDD("/rdd/mapToPair", table, lambda);
		return new FlamePairRDDImpl(kvs, outTable);
	}

	@Override
	public FlameRDD intersection(FlameRDD other) throws Exception {
		checkAlive();
		String otherTable = ((FlameRDDImpl) other).table;
		String extra = "other=" + URLEncoder.encode(otherTable, "UTF-8");
		String outTable = Coordinator.startRDD("/rdd/intersection", this.table, null, extra);
		return new FlameRDDImpl(kvs, outTable);
	}

	@Override
	public FlameRDD sample(double f) throws Exception {
		checkAlive();
		if (f <= 0.0) {
			String out = "flame-" + UUID.randomUUID();
			return new FlameRDDImpl(kvs, out);
		}
		String extra = "p=" + URLEncoder.encode(Double.toString(f), "UTF-8");
		String outTable = Coordinator.startRDD("/rdd/sample", this.table, null, extra);
		return new FlameRDDImpl(kvs, outTable);
	}

	@Override
	public FlamePairRDD groupBy(FlameRDD.StringToString L) throws Exception {
		checkAlive();
		FlamePairRDD pairs = this.mapToPair(s -> new FlamePair(L.op(s), s));
		return pairs.foldByKey("", (a, b) -> (a == null || a.isEmpty()) ? b : a + "," + b);
	}

	@Override
	public int count() throws Exception {
		checkAlive();
		return kvs.count(table);
	}

	@Override
	public void saveAsTable(String tableName) throws Exception {
		checkAlive();
		var it = kvs.scan(table);
		while (it.hasNext()) {
			var row = it.next();
			var val = row.get("value");
			if (val != null) {
				kvs.put(tableName, row.key(), "value", val);
			}
		}
	}

	@Override
	public FlameRDD distinct() throws Exception {
		checkAlive();
		String outTable = Coordinator.startRDD("/rdd/distinct", table, null);
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
	public Vector<String> take(int num) throws Exception {
		checkAlive();
		Vector<String> out = new Vector<>();
		if (num <= 0) {
			return out;
		}

		var it = kvs.scan(table);
		while (it.hasNext() && out.size() < num) {
			var row = it.next();
			var val = row.get("value");
			if (val != null) {
				out.add(val);
			}
		}
		return out;
	}

	@Override
	public String fold(String zeroElement, TwoStringsToString lambda) throws Exception {
		checkAlive();
		String zero = "";
		if (zeroElement != null) {
			zero = zeroElement;
		}
		byte[] f = Serializer.objectToByteArray(lambda);
		return Coordinator.fold(table, f, lambda, zero);
	}

	@Override
	public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
		checkAlive();
		byte[] f = Serializer.objectToByteArray(lambda);
		String outTable = Coordinator.startRDD("/rdd/flatMapToPair", table, f);
		return new FlamePairRDDImpl(kvs, outTable);
	}

	@Override
	public FlameRDD filter(StringToBoolean lambda) throws Exception {
		checkAlive();
		byte[] f = Serializer.objectToByteArray(lambda);
		String outTable = Coordinator.startRDD("/rdd/filter", table, f);
		return new FlameRDDImpl(kvs, outTable);
	}

	@Override
	public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
		checkAlive();
		byte[] f = Serializer.objectToByteArray(lambda);
		String outTable = Coordinator.startRDD("/rdd/mapPartitions", table, f);
		return new FlameRDDImpl(kvs, outTable);
	}
}
