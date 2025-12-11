package cis5550.flame;

import java.io.Serializable;
import java.util.Vector;
import java.util.Iterator;
import java.util.List;

public interface FlameRDD {
	public interface StringToIterable extends Serializable {
		Iterable<String> op(String a) throws Exception;
	};

	public interface StringToPair extends Serializable {
		FlamePair op(String a) throws Exception;
	};

	public interface StringToPairIterable extends Serializable {
		Iterable<FlamePair> op(String a) throws Exception;
	};

	public interface StringToString extends Serializable {
		String op(String a) throws Exception;
	};

	public interface StringToBoolean extends Serializable {
		boolean op(String a) throws Exception;
	}

	public interface IteratorToIterator extends Serializable {
		Iterator<String> op(Iterator<String> a) throws Exception;
	}

	public int count() throws Exception;

	public void saveAsTable(String tableNameArg) throws Exception;

	public FlameRDD distinct() throws Exception;

	public void destroy() throws Exception;

	public Vector<String> take(int num) throws Exception;

	public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception;

	public List<String> collect() throws Exception;

	public FlameRDD flatMap(StringToIterable lambda) throws Exception;

	public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception;

	public FlamePairRDD mapToPair(StringToPair lambda) throws Exception;

	public FlameRDD intersection(FlameRDD r) throws Exception;

	public FlameRDD sample(double f) throws Exception;

	public FlamePairRDD groupBy(StringToString lambda) throws Exception;

	public FlameRDD filter(StringToBoolean lambda) throws Exception;

	public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception;
}
