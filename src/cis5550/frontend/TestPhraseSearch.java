package cis5550.frontend;

import cis5550.kvs.*;
import java.util.*;

public class TestPhraseSearch {
    public static void main(String[] args) throws Exception {
        KVSClient kvs = new KVSClient("localhost:8000");

        // setup minimal pt-index data
        // Term "space" appears in both docs
        Row spaceRow = new Row("space");
        spaceRow.put("value",
            "1.0\t2\t" +
            "http://doc1.com|tf=2|weightedTf=1.0|normalizedTf=0.5|docLen=10|pos=1 3|weights=1.0 1.0|" +
            ", " +
            "http://doc2.com|tf=2|weightedTf=1.0|normalizedTf=0.5|docLen=10|pos=1 10|weights=1.0 1.0|"
        );
        kvs.putRow("pt-index", spaceRow);

        Row travelRow = new Row("travel");
        travelRow.put("value",
            "1.0\t2\t" +
            "http://doc1.com|tf=2|weightedTf=1.0|normalizedTf=0.5|docLen=10|pos=2 7|weights=1.0 1.0|" +
            ", " +
            "http://doc2.com|tf=2|weightedTf=1.0|normalizedTf=0.5|docLen=10|pos=5 15|weights=1.0 1.0|"
        );
        kvs.putRow("pt-index", travelRow);
        // set pagerank scores 
        Row pr1 = new Row(cis5550.tools.Hasher.hash("http://doc1.com"));
        pr1.put("rank", "0.2");
        kvs.putRow("pt-pageranks", pr1);

        Row pr2 = new Row(cis5550.tools.Hasher.hash("http://doc2.com"));
        pr2.put("rank", "0.2");
        kvs.putRow("pt-pageranks", pr2);

        // run search
        List<SearchEngine.SearchResult> results = SearchEngine.executeSearch(kvs, "space travel", 10);

        // print results
        for (SearchEngine.SearchResult r : results) {
            System.out.printf("%s — total=%.5f (tfidf=%.5f, pagerank=%.5f)%n",
                r.url, r.totalScore, r.tfidfScore, r.pageRankScore);
        }
    }
}
