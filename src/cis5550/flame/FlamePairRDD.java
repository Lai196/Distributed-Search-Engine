package cis5550.flame;

import java.util.List;
import java.util.Iterator;
import java.io.Serializable;

public interface FlamePairRDD {
	public interface TwoStringsToString extends Serializable {
		public String op(String a, String b);
	};

	public interface StringAccumulator extends Serializable {
		public void op(StringBuilder acc, String v);
	}

	public interface PairToPairIterable extends Serializable {
		Iterable<FlamePair> op(FlamePair a) throws Exception;
	};

	public interface PairToStringIterable extends Serializable {
		Iterable<String> op(FlamePair a) throws Exception;
	};

	public List<FlamePair> collect() throws Exception;

	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception;

	public FlamePairRDD foldByKey(String zeroElement, StringAccumulator lambda) throws Exception;

	public void saveAsTable(String tableNameArg) throws Exception;

	public FlameRDD flatMap(PairToStringIterable lambda) throws Exception;

	public void destroy() throws Exception;

	public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception;

	public FlamePairRDD join(FlamePairRDD other) throws Exception;

	public FlamePairRDD cogroup(FlamePairRDD other) throws Exception;
}
