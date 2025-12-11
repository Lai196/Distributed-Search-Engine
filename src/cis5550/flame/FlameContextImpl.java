package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.tools.Serializer;

import java.util.List;
import java.util.UUID;

class FlameContextImpl implements FlameContext {
	private final KVSClient kvs;
	private final StringBuilder out = new StringBuilder();

	FlameContextImpl(KVSClient kvs) {
		this.kvs = kvs;
	}

	@Override
	public KVSClient getKVS() {
		return kvs;
	}

	@Override
	public synchronized void output(String s) {
		if (s != null) {
			out.append(s);
		}
	}

	String getOutputOrDefault() {
		return (out.length() == 0) ? "No output" : out.toString();
	}

	@Override
	public FlameRDD parallelize(List<String> list) throws Exception {

		String tableName = "flame-" + UUID.randomUUID().toString();

		int rowId = 0;
		for (String value : list) {
			kvs.put(tableName, String.valueOf(rowId++), "value", value);
		}

		return new FlameRDDImpl(kvs, tableName);
	}

	@Override
	public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
		byte[] lambdaBytes = Serializer.objectToByteArray(lambda);
		String outTable = Coordinator.startRDD("/ctx/fromTable", tableName, lambdaBytes);
		return new FlameRDDImpl(kvs, outTable);
	}

	@Override
	public void setConcurrencyLevel(int keyRangesPerWorker) {
		if (keyRangesPerWorker < 1)
			keyRangesPerWorker = 1;
		Coordinator.setConcurrency(keyRangesPerWorker);
	}
}
