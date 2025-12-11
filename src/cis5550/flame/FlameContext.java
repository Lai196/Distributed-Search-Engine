package cis5550.flame;

import java.util.*;
import java.io.*;
import cis5550.kvs.Row;
import cis5550.kvs.KVSClient;

public interface FlameContext {
	public KVSClient getKVS();

	public interface RowToString extends Serializable {
		String op(Row r);
	};

	public void output(String s);

	public FlameRDD parallelize(List<String> list) throws Exception;

	public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception;

	public void setConcurrencyLevel(int keyRangesPerWorker);
}
