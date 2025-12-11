package cis5550.jobs;

import cis5550.flame.*;
import cis5550.kvs.Row;
import cis5550.kvs.KVSClient;
// import cis5550.external.PorterStemmer; // Import stemmer for the EC requirements
import cis5550.tools.Hasher;
import java.util.*;
import java.util.concurrent.*;
import opennlp.tools.stemmer.PorterStemmer;
import java.io.*;
import java.nio.charset.StandardCharsets;

public class Indexer {
    private static final String CHECKPOINT_TABLE = "pt-documentStats";
    private static final String BATCH_CHECKPOINT_TABLE = "pt-indexer-batch-checkpoint";
    private static final int MAX_STRING_SIZE = 500_000_000; // 500MB limit for regular terms
    private static final int MAX_STRING_SIZE_STOP_WORD = 50_000_000; // 50MB limit for stop words
    private static final int MAX_DOCS_PER_TERM = 20000; // Max documents per regular term
    private static final int MAX_DOCS_PER_STOP_WORD = 5000; // Max documents per stop word
    private static final int MAX_SAFE_STRING_SIZE = 1_500_000_000;

    // Flag to disable verbose logging in hot paths for performance
    private static final boolean ENABLE_VERBOSE_LOGGING = false;
    
    // ThreadLocal PorterStemmer to reuse stemmer instances per thread
    private static final ThreadLocal<PorterStemmer> STEMMER_CACHE = ThreadLocal.withInitial(PorterStemmer::new);
    
    // Fast formatting to replace slow String.format() calls
    private static String formatDouble3Fast(double d) {
        // Faster than String.format("%.3f", d)
        if (Double.isNaN(d) || Double.isInfinite(d)) return "0.000";
        long scaled = Math.round(d * 1000);
        boolean negative = scaled < 0;
        if (negative) scaled = -scaled;
        long integer = scaled / 1000;
        long fraction = scaled % 1000;
        String result = integer + "." + String.format("%03d", fraction);
        return negative ? "-" + result : result;
    }
    
    private static String formatDouble4Fast(double d) {
        // Faster than String.format("%.4f", d)
        if (Double.isNaN(d) || Double.isInfinite(d)) return "0.0000";
        long scaled = Math.round(d * 10000);
        boolean negative = scaled < 0;
        if (negative) scaled = -scaled;
        long integer = scaled / 10000;
        long fraction = scaled % 10000;
        String result = integer + "." + String.format("%04d", fraction);
        return negative ? "-" + result : result;
    }
    
    private static String formatDouble6Fast(double d) {
        // Faster than String.format("%.6f", d)
        if (Double.isNaN(d) || Double.isInfinite(d)) return "0.000000";
        long scaled = Math.round(d * 1000000);
        boolean negative = scaled < 0;
        if (negative) scaled = -scaled;
        long integer = scaled / 1000000;
        long fraction = scaled % 1000000;
        String result = integer + "." + String.format("%06d", fraction);
        return negative ? "-" + result : result;
    }
    
    private static String formatDouble8Fast(double d) {
        if (Double.isNaN(d) || Double.isInfinite(d)) return "0.00000000";
        long scaled = Math.round(d * 100000000L);
        boolean negative = scaled < 0;
        if (negative) scaled = -scaled;
        long integer = scaled / 100000000L;
        long fraction = scaled % 100000000L;
        String result = integer + "." + String.format("%08d", fraction);
        return negative ? "-" + result : result;
    }

    private static final java.util.regex.Pattern SCRIPT_TAG = java.util.regex.Pattern
            .compile("(?i)(?s)<script[^>]*>.*?</script>");
    private static final java.util.regex.Pattern STYLE_TAG = java.util.regex.Pattern
            .compile("(?i)(?s)<style[^>]*>.*?</style>");
    private static final java.util.regex.Pattern HTML_COMMENT = java.util.regex.Pattern.compile("(?s)<!--.*?-->");
    private static final java.util.regex.Pattern META_TAG = java.util.regex.Pattern.compile("(?i)(?s)<meta[^>]*>");
    private static final java.util.regex.Pattern SCRIPT_JSON = java.util.regex.Pattern
            .compile("(?i)(?s)<script[^>]*type\\s*=\\s*[\"']application/json[\"'][^>]*>.*?</script>");
    private static final java.util.regex.Pattern SCRIPT_JSON_LD = java.util.regex.Pattern
            .compile("(?i)(?s)<script[^>]*type\\s*=\\s*[\"']application/ld\\+json[\"'][^>]*>.*?</script>");
    private static final java.util.regex.Pattern HTML_TAGS = java.util.regex.Pattern.compile("(?s)<[^>]*>");
    private static final java.util.regex.Pattern NON_ALPHANUMERIC = java.util.regex.Pattern.compile("[^A-Za-z0-9\\s]");
    private static final java.util.regex.Pattern SCRIPT_CONTENT = java.util.regex.Pattern
            .compile("(?i)(?s)<script[^>]*>(.*?)</script>");
    private static final java.util.regex.Pattern STYLE_CONTENT = java.util.regex.Pattern
            .compile("(?i)(?s)<style[^>]*>(.*?)</style>");
    private static final java.util.regex.Pattern WHITESPACE = java.util.regex.Pattern.compile("\\s+");
    private static final java.util.regex.Pattern TITLE_TAG = java.util.regex.Pattern
            .compile("(?i)(?s)<title[^>]*>(.*?)</title>");

    public static void run(FlameContext ctx, String[] args) throws Exception {
        ctx.output("Starting Indexer with Word Positions + Stemming EC...\n");
        System.out
                .println("[Indexer] This is starting the Indexer with Stemming and Word Position tracking...");
        long totalStart = System.currentTimeMillis();

        final Set<String> stopWords = loadStopWords();

        boolean shouldResume = false;
        Set<String> processedUrls = new HashSet<>();
        Set<Integer> completedBuckets = new HashSet<>();

        // Parse command-line arguments
        boolean userWantsResume = false;
        int NUM_HASH_BUCKETS = 100; // Default bucket count
        int argOffset = 0;

        if (args != null && args.length > 0) {
            if (args[0].equalsIgnoreCase("resume")) {
                userWantsResume = true;
                argOffset = 1;
            }
            // check for bucket count arg
            if (args.length > argOffset) {
                try {
                    NUM_HASH_BUCKETS = Integer.parseInt(args[argOffset]);
                    ctx.output("[INFO] Using " + NUM_HASH_BUCKETS + " buckets\n");
                    System.out.println("[Indexer] Using " + NUM_HASH_BUCKETS + " hash buckets");
                } catch (NumberFormatException e) {
                    ctx.output("[WARNING] Invalid bucket count, using default: " + NUM_HASH_BUCKETS + "\n");
                }
            }
        }

        final int FINAL_NUM_HASH_BUCKETS = NUM_HASH_BUCKETS; // Make final for lambda access

        try {
            if (userWantsResume) {
                // load completed buckets
                try {
                    Iterator<Row> bucketIter = ctx.getKVS().scan(BATCH_CHECKPOINT_TABLE);
                    while (bucketIter.hasNext()) {
                        Row r = bucketIter.next();
                        String bucketStr = r.get("bucket");
                        if (bucketStr != null) {
                            try {
                                completedBuckets.add(Integer.parseInt(bucketStr));
                            } catch (NumberFormatException ignored) {
                            }
                        }
                    }
                } catch (Exception e) {
                }

                // see what was successfully indexed
                Iterator<Row> docStatsIter = ctx.getKVS().scan("pt-documentStats");
                int loadedCount = 0;
                while (docStatsIter.hasNext()) {
                    Row r = docStatsIter.next();
                    String url = r.get("url");
                    if (url != null && !url.isEmpty()) {
                        processedUrls.add(url);
                        loadedCount++;
                    }
                }

                if (loadedCount > 0 || !completedBuckets.isEmpty()) {
                    shouldResume = true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        final Set<String> finalProcessedUrls = processedUrls;
        final boolean resumeMode = shouldResume;

        long t1 = System.currentTimeMillis();
        FlameRDD crawlData = ctx.fromTable("pt-crawl", (Row row) -> {
            String url = row.get("url");
            String page = row.get("page");

            if (url == null || page == null) {
                return null;
            }

            if (resumeMode && finalProcessedUrls.contains(url)) {
                return null;
            }

            return url + "," + page;
        });
        ctx.output("[TIMER] fromTable loaded in " + (System.currentTimeMillis() - t1) + " ms\n");

        // -----------------------------------------------------------------------
        // Compute doc lengths (needed for normalized TF + pt-documentStats)
        // -----------------------------------------------------------------------

        long t2 = System.currentTimeMillis();
        FlameRDD docLenRecords = crawlData.flatMap(s -> {
            List<String> out = new ArrayList<>();
            if (s == null) {
                return out;
            }
            int commaPos = s.indexOf(',');
            if (commaPos < 0) {
                return out;
            }
            String url = s.substring(0, commaPos).trim();
            String page = s.substring(commaPos + 1);

            if (page.length() > 1_000_000) { 
                System.out.println("[Indexer] Skipping oversized page ("
                        + page.length() + " chars): " + url);
                return out;
            }

            List<String> tokens = tokenize(page); // Tokenize page, return ("url,length")
            Map<String, Integer> termFreq = new HashMap<>();
            for (String tok : tokens) {
                termFreq.put(tok, termFreq.getOrDefault(tok, 0) + 1);
            }
            // Use simple loop instead of stream for better performance
            int maxTf = 1;
            for (int freq : termFreq.values()) {
                if (freq > maxTf) {
                    maxTf = freq;
                }
            }
            String title = extractTitle(page);
            // String preview = extractPreview(page);
            out.add(url + "," + tokens.size() + "," + maxTf + "," + title);
            return out;
        });
        ctx.output("[TIMER] flatMap(docLen) done in " + (System.currentTimeMillis() - t2) + " ms\n");

        long t2b = System.currentTimeMillis();
        List<String> docLengthList = docLenRecords.collect();
        ctx.output("[TIMER] collect() took " + (System.currentTimeMillis() - t2b) + " ms\n");

        Map<String, Integer> docLenMap = new HashMap<>();
        Map<String, Integer> maxTfMap = new HashMap<>();
        for (String record : docLengthList) {
            if (record == null || record.isEmpty())
                continue;

            int comma1 = record.indexOf(',');
            if (comma1 < 0)
                continue;
            int comma2 = record.indexOf(',', comma1 + 1);
            if (comma2 < 0)
                continue;
            int comma3 = record.indexOf(',', comma2 + 1);

            String url = record.substring(0, comma1);
            String lenStr = record.substring(comma1 + 1, comma2);
            String maxTfStr = (comma3 > 0) ? record.substring(comma2 + 1, comma3) : record.substring(comma2 + 1);

            try {
                docLenMap.put(url, Integer.parseInt(lenStr));
                maxTfMap.put(url, Integer.parseInt(maxTfStr));
            } catch (NumberFormatException ignored) {
                docLenMap.put(url, 0);
                maxTfMap.put(url, 1);
            }
        }

        int totalDocsCount = docLenMap.size();
        // Always check for existing documents in pt-documentStats for accurate IDF
        // calculation
        // This ensures IDF is correct even if pt-documentStats has entries from
        // previous runs
        try {
            int alreadyIndexedCount = ctx.getKVS().count("pt-documentStats");
            if (alreadyIndexedCount > 0) {
                totalDocsCount = alreadyIndexedCount + docLenMap.size();
                if (resumeMode) {
                    ctx.output("[INFO] Total documents for IDF: " + alreadyIndexedCount + " previously indexed + "
                            + docLenMap.size() + " new = " + totalDocsCount + "\n");
                } else {
                    ctx.output("[INFO] Total documents for IDF: " + alreadyIndexedCount + " existing + "
                            + docLenMap.size() + " new = " + totalDocsCount + "\n");
                }
            } else {
                // No existing documents, just use current batch
                if (resumeMode) {
                    ctx.output("[INFO] Total documents for IDF: 0 previously indexed + " + docLenMap.size() + " new = "
                            + totalDocsCount + "\n");
                } else {
                    ctx.output("[INFO] Total documents counted: " + totalDocsCount + "\n");
                }
            }
        } catch (Exception e) {
            // If count fails, just use current batch
            if (resumeMode) {
                ctx.output("[WARNING] Could not count existing documents, using current batch count: " + e.getMessage()
                        + "\n");
            } else {
                ctx.output("[INFO] Total documents counted: " + totalDocsCount + "\n");
            }
        }

        final int totalDocs = totalDocsCount;
        final List<String> finalDocLengthList = docLengthList;

        // -----------------------------------------------------------------------
        // HASH BUCKET PROCESSING LOOP: Process documents in hash-based buckets
        // This allows periodic checkpoints during heavy processing phases
        // -----------------------------------------------------------------------

        final Set<Integer> finalCompletedBuckets = completedBuckets;

        System.out.println("Starting bucket processing (total buckets: " + FINAL_NUM_HASH_BUCKETS + ")");

        // Pre-load document stats cache once before the bucket loop
        Map<String, Integer> docLenCache = new ConcurrentHashMap<>();
        Map<String, Integer> maxTfCache = new ConcurrentHashMap<>();
        Map<String, String> urlHashCache = new ConcurrentHashMap<>();

        // Load from pt-documentStats ONCE (outside bucket loop)
        System.out.println("[INFO] Pre-loading document stats cache...");
        Iterator<Row> docStatsIter = ctx.getKVS().scan("pt-documentStats");
        int cachedCount = 0;
        while (docStatsIter.hasNext()) {
            Row r = docStatsIter.next();
            String lenStr = r.get("length");
            String maxTfStr = r.get("maxTf");
            if (lenStr != null) {
                try {
                    docLenCache.put(r.key(), Integer.parseInt(lenStr));
                } catch (NumberFormatException ignored) {
                }
            }
            if (maxTfStr != null) {
                try {
                    maxTfCache.put(r.key(), Integer.parseInt(maxTfStr));
                } catch (NumberFormatException ignored) {
                }
            }
            cachedCount++;
        }
        System.out.println("[INFO] Loaded " + cachedCount + " document stats entries");

        // Populate cache from docLenMap using hashed URLs as keys
        // urlHashCache populated on-demand per bucket to avoid memory leak
        for (Map.Entry<String, Integer> entry : docLenMap.entrySet()) {
            try {
                String url = entry.getKey();
                String hashResult = Hasher.hash(url);
                int newlineIdx = hashResult.indexOf('\n');
                String hashed = (newlineIdx > 0) ? hashResult.substring(0, newlineIdx) : hashResult;

                if (hashed != null) {
                    docLenCache.put(hashed, entry.getValue());
                    Integer maxTf = maxTfMap.get(url);
                    if (maxTf != null) {
                        maxTfCache.put(hashed, maxTf);
                    }
                }
            } catch (Exception ignored) {
            }
        }

        final Map<String, String> finalUrlHashCache = urlHashCache;

        // Accumulate timer values across all buckets
        long totalFlatMapTime = 0;
        long totalFoldByKeyTime = 0;
        long totalTfIdfTime = 0;
        long totalSaveTime = 0;

        for (int bucketNum = 0; bucketNum < FINAL_NUM_HASH_BUCKETS; bucketNum++) {
            if (finalCompletedBuckets.contains(bucketNum)) {
                continue;
            }

            System.out.println("Running bucket " + bucketNum + "/" + (FINAL_NUM_HASH_BUCKETS - 1));

            // track saved URLs in this bucket to prevent duplicates
            Set<String> savedUrlsInBucket = ConcurrentHashMap.newKeySet();
            final int currentBucket = bucketNum;

            // filter crawlData to only include URLs in this hash bucket
            FlameRDD bucketCrawlData = crawlData.filter(s -> {
                if (s == null)
                    return false;
                int commaPos = s.indexOf(',');
                if (commaPos < 0)
                    return false;
                String url = s.substring(0, commaPos).trim();

                // hash the URL and map to bucket number
                try {
                    int urlHash = Math.abs(url.hashCode());
                    int urlBucket = urlHash % FINAL_NUM_HASH_BUCKETS;
                    return urlBucket == currentBucket;
                } catch (Exception e) {
                    return false;
                }
            });

            // -----------------------------------------------------------------------
            // Map pages -> (word, url|pos=...) pairs (Aaron's original pipeline logic)
            // -----------------------------------------------------------------------

            long t3 = System.currentTimeMillis();
            FlamePairRDD wordUrlPosPairs = bucketCrawlData.flatMapToPair(s -> {
                List<FlamePair> out = new ArrayList<>();
                if (s == null) {
                    return out;
                }
                int commaPos = s.indexOf(',');
                if (commaPos < 0) {
                    return out;
                }
                String url = s.substring(0, commaPos).trim();
                String page = s.substring(commaPos + 1);

                if (page.length() > 1_000_000) {
                    if (ENABLE_VERBOSE_LOGGING) {
                        System.out.println("[Indexer] Skipping oversized page ("
                                + page.length() + " chars): " + url);
                    }
                    return out;
                }

                List<String> weightedToks = tokenizeWithWeights(page); 
                PorterStemmer stemmer = STEMMER_CACHE.get();

                // Map from term to list of (position, weight) pairs
                Map<String, List<Map.Entry<Integer, Double>>> termToPositionsWithWeights = new HashMap<>();
                int position = 1; // 1 based position counter

                // This for loop will record word positions (typically in 1-based format) with
                // weights
                for (String weightedTok : weightedToks) {
                    if (weightedTok == null || weightedTok.isEmpty())
                        continue;

                    // Parse token and weight: "token|weight"
                    int pipeIdx = weightedTok.indexOf('|');
                    if (pipeIdx < 0)
                        continue;

                    String tok = weightedTok.substring(0, pipeIdx).trim();
                    if (tok.isEmpty())
                        continue;

                    double weight;
                    try {
                        weight = Double.parseDouble(weightedTok.substring(pipeIdx + 1));
                    } catch (NumberFormatException e) {
                        weight = 1.0;
                    }

                    String tokLower = tok.toLowerCase();
                    if (tokLower.length() < 2) {
                        position++;
                        continue;
                    }

                    if (!isWorthIndexing(tokLower)) {
                        position++;
                        continue;
                    }

                    boolean isStopWord = stopWords.contains(tokLower);

                    // Store position with weight
                    termToPositionsWithWeights.computeIfAbsent(tokLower, k -> new ArrayList<>())
                            .add(new java.util.AbstractMap.SimpleEntry<>(position, weight));

                    if (!isStopWord) {
                        String stemmed = stemmer.stem(tokLower);
                        if (stemmed != null && !stemmed.isEmpty() && !stemmed.equals(tokLower)) {
                            termToPositionsWithWeights.computeIfAbsent(stemmed, k -> new ArrayList<>())
                                    .add(new java.util.AbstractMap.SimpleEntry<>(position, weight));
                        }
                    }

                    position++;
                }

                // Convert to format: "url|pos=1 2 3|weights=1.0 0.005 1.0"
                for (Map.Entry<String, List<Map.Entry<Integer, Double>>> e : termToPositionsWithWeights.entrySet()) {
                    List<Map.Entry<Integer, Double>> posWeightList = e.getValue();

                    // Sort by position
                    posWeightList.sort((a, b) -> Integer.compare(a.getKey(), b.getKey()));

                    int listSize = posWeightList.size();
                    List<Integer> posList = new ArrayList<>(listSize);
                    List<Double> weightList = new ArrayList<>(listSize);
                    for (Map.Entry<Integer, Double> pw : posWeightList) {
                        posList.add(pw.getKey());
                        weightList.add(pw.getValue());
                    }

                    int estimatedPosSize = posList.size() * 5;
                    int estimatedWeightSize = weightList.size() * 7;
                    StringBuilder posBuilder = new StringBuilder(estimatedPosSize);
                    for (int i = 0; i < posList.size(); i++) {
                        if (i > 0)
                            posBuilder.append(" ");
                        posBuilder.append(posList.get(i));
                    }
                    String posStr = posBuilder.toString();

                    StringBuilder weightBuilder = new StringBuilder(estimatedWeightSize);
                    for (int i = 0; i < weightList.size(); i++) {
                        if (i > 0)
                            weightBuilder.append(" ");
                        weightBuilder.append(formatDouble3Fast(weightList.get(i)));
                    }
                    String weightStr = weightBuilder.toString();

                    out.add(new FlamePair(e.getKey(), url + "|pos=" + posStr + "|weights=" + weightStr));
                }

                // System.out.println("[Aaron’s Indexer] This finished tokenizing: " + url + "
                // (" + toks.size() + " words)");
                return out;
            });
            totalFlatMapTime += (System.currentTimeMillis() - t3);

            // -----------------------------------------------------------------------------------------------------------------------------
            // foldByKey to merge all docs for same word ( url1|pos=2 5 and url2|pos=8 20 ->
            // single value url1|pos=2 5,url2|pos=8 20 )
            // -----------------------------------------------------------------------------------------------------------------------------

            long t4 = System.currentTimeMillis();
            FlamePairRDD wordToUrlPosList = wordUrlPosPairs.foldByKey("", (a, b) -> {
                if (a == null || a.isEmpty()) {
                    return b;
                }
                if (b == null || b.isEmpty()) {
                    return a;
                }
                return a + "," + b;
            });
            totalFoldByKeyTime += (System.currentTimeMillis() - t4);

            final String kvsCoord = ctx.getKVS().getCoordinator();

            // -----------------------------------------------------------------------
            // Aggregate TF/IDF + normalized TF + stop flag
            // -----------------------------------------------------------------------

            long t5 = System.currentTimeMillis();
            final double ALPHA = 0.4;
            FlamePairRDD aggWordToUrlPosList = wordToUrlPosList.flatMapToPair(pair -> {

                String word = pair._1();
                String merged = pair._2();
                if (merged == null || merged.isEmpty()) {
                    return Collections.emptyList();
                }

                // Map from URL to list of (position, weight) pairs
                Map<String, List<Map.Entry<Integer, Double>>> urlToPositionsWithWeights = new HashMap<>();

                // Parse entries without split for better performance
                int entryStart = 0;
                while (entryStart < merged.length()) {
                    int entryEnd = merged.indexOf(',', entryStart);
                    if (entryEnd == -1)
                        entryEnd = merged.length();

                    String entry = merged.substring(entryStart, entryEnd).trim();
                    entryStart = entryEnd + 1;

                    if (entry.isEmpty())
                        continue;

                    // Parse fields without split
                    int pipe1 = entry.indexOf('|');
                    if (pipe1 < 0)
                        continue;
                    String url = entry.substring(0, pipe1);

                    String posStr = "";
                    String weightsStr = "";

                    // Parse remaining fields (pos=... and weights=...)
                    int fieldStart = pipe1 + 1;
                    while (fieldStart < entry.length()) {
                        int fieldEnd = entry.indexOf('|', fieldStart);
                        if (fieldEnd == -1)
                            fieldEnd = entry.length();

                        String field = entry.substring(fieldStart, fieldEnd);
                        if (field.startsWith("pos=")) {
                            posStr = field.substring(4);
                        } else if (field.startsWith("weights=")) {
                            weightsStr = field.substring(8);
                        }

                        fieldStart = (fieldEnd == entry.length()) ? entry.length() : fieldEnd + 1;
                    }

                    if (posStr.isEmpty())
                        continue;

                    // Parse positions and weights without split
                    List<Integer> positions = new ArrayList<>();
                    List<Double> weights = new ArrayList<>();

                    // Parse positions
                    int posStart = 0;
                    while (posStart < posStr.length()) {
                        while (posStart < posStr.length() && posStr.charAt(posStart) == ' ')
                            posStart++;
                        if (posStart >= posStr.length())
                            break;
                        int posEnd = posStart;
                        while (posEnd < posStr.length() && posStr.charAt(posEnd) != ' ')
                            posEnd++;
                        try {
                            positions.add(Integer.parseInt(posStr.substring(posStart, posEnd)));
                        } catch (NumberFormatException ignored) {
                        }
                        posStart = posEnd + 1;
                    }

                    // Parse weights
                    if (!weightsStr.isEmpty()) {
                        int weightStart = 0;
                        while (weightStart < weightsStr.length()) {
                            while (weightStart < weightsStr.length() && weightsStr.charAt(weightStart) == ' ')
                                weightStart++;
                            if (weightStart >= weightsStr.length())
                                break;
                            int weightEnd = weightStart;
                            while (weightEnd < weightsStr.length() && weightsStr.charAt(weightEnd) != ' ')
                                weightEnd++;
                            try {
                                weights.add(Double.parseDouble(weightsStr.substring(weightStart, weightEnd)));
                            } catch (NumberFormatException ignored) {
                            }
                            weightStart = weightEnd + 1;
                        }
                    }

                    List<Map.Entry<Integer, Double>> posWeightList = urlToPositionsWithWeights.computeIfAbsent(url,
                            k -> new ArrayList<>());

                    for (int i = 0; i < positions.size(); i++) {
                        int pos = positions.get(i);
                        double weight = (i < weights.size()) ? weights.get(i) : 1.0;
                        posWeightList.add(new java.util.AbstractMap.SimpleEntry<>(pos, weight));
                    }
                }

                List<Map.Entry<String, List<Map.Entry<Integer, Double>>>> sorted = new ArrayList<>(
                        urlToPositionsWithWeights.entrySet());

                sorted.removeIf(e -> e.getValue() == null || e.getValue().isEmpty());
                // Sort positions within each URL
                for (Map.Entry<String, List<Map.Entry<Integer, Double>>> e : sorted) {
                    e.getValue().sort((a, b) -> Integer.compare(a.getKey(), b.getKey()));
                }

                // Sort by weighted TF (sum of weights) descending
                Map<String, Double> urlToWeightSum = new HashMap<>();
                for (Map.Entry<String, List<Map.Entry<Integer, Double>>> e : sorted) {
                    double sum = 0.0;
                    for (Map.Entry<Integer, Double> pw : e.getValue()) {
                        sum += pw.getValue();
                    }
                    urlToWeightSum.put(e.getKey(), sum);
                }
                sorted.sort((e1, e2) -> {
                    double w1 = urlToWeightSum.getOrDefault(e1.getKey(), 0.0);
                    double w2 = urlToWeightSum.getOrDefault(e2.getKey(), 0.0);
                    return Double.compare(w2, w1);
                });

                String wordLower = word.toLowerCase();
                final boolean isStopWord = stopWords.contains(wordLower);

                // limit number of documents before processing
                int maxDocs = isStopWord ? MAX_DOCS_PER_STOP_WORD : MAX_DOCS_PER_TERM;
                if (sorted.size() > maxDocs) {
                    System.out.println("[Indexer] Term '" + word + "' truncated from " + sorted.size() + " to "
                            + maxDocs + " docs (count limit)");
                    sorted = sorted.subList(0, maxDocs);
                }

                List<String> urlParts = new ArrayList<>(sorted.size());
                for (Map.Entry<String, List<Map.Entry<Integer, Double>>> e : sorted) {

                    String url = e.getKey();
                    List<Map.Entry<Integer, Double>> posWeightList = e.getValue();

                    // calculate weighted TF (sum of weights)
                    double weightedTf = 0.0; // directly sum the weights
                    for (Map.Entry<Integer, Double> pw : posWeightList) {
                        weightedTf += pw.getValue();
                    }
                    int tf = posWeightList.size();

                    // extract positions and weights for storage (e.g. [1, 19, 98] and [1.0, 0.005,
                    // 1.0])
                    int listSize = posWeightList.size();
                    List<Integer> posList = new ArrayList<>(listSize);
                    List<Double> weightList = new ArrayList<>(listSize);
                    for (Map.Entry<Integer, Double> pw : posWeightList) {
                        posList.add(pw.getKey());
                        weightList.add(pw.getValue());
                    }

                    // lookup document length from KVS on-demand
                    int docLen = 0;
                    int maxTf = 1;
                    try {
                        String hashResult = Hasher.hash(url);
                        int newlineIdx = hashResult.indexOf('\n');
                        String hashed = (newlineIdx > 0) ? hashResult.substring(0, newlineIdx) : hashResult;
                        
                        KVSClient kvs = new KVSClient(kvsCoord);
                        Row docStatsRow = kvs.getRow("pt-documentStats", hashed);
                        if (docStatsRow != null) {
                            String lenStr = docStatsRow.get("length");
                            String maxTfStr = docStatsRow.get("maxTf");
                            if (lenStr != null) {
                                try {
                                    docLen = Integer.parseInt(lenStr);
                                } catch (NumberFormatException ignored) {}
                            }
                            if (maxTfStr != null) {
                                try {
                                    maxTf = Integer.parseInt(maxTfStr);
                                } catch (NumberFormatException ignored) {}
                            }
                        }
                    } catch (Exception ex) {
                    }

                    // Use weighted TF for normalization instead of raw count
                    // Normalized TF uses weighted TF divided by max weighted TF for the document
                    // For maxTf, we still use the raw count-based maxTf, but we can scale
                    // weightedTf appropriately
                    double normalizedTf = (weightedTf > 0 && maxTf > 0) ? (ALPHA + (1.0 - ALPHA) * (weightedTf) / maxTf)
                            : 0.0;

                    int estimatedPosSize = posList.size() * 5;
                    int estimatedWeightSize = weightList.size() * 7;
                    StringBuilder posBuilder = new StringBuilder(estimatedPosSize);
                    for (int i = 0; i < posList.size(); i++) {
                        if (i > 0)
                            posBuilder.append(" ");
                        posBuilder.append(posList.get(i));
                    }
                    String posStr = posBuilder.toString();

                    StringBuilder weightBuilder = new StringBuilder(estimatedWeightSize);
                    for (int i = 0; i < weightList.size(); i++) {
                        if (i > 0)
                            weightBuilder.append(" ");
                        weightBuilder.append(formatDouble3Fast(weightList.get(i)));
                    }
                    String weightStr = weightBuilder.toString();

                    // for example: url|tf=3|weightedTf=2.4|normalizedTf=0.012345|docLen=243|pos=1
                    // 19 98|weights=1.0 0.005 1.0
                    // normalized tf uses weighted TF for better accuracy with visible text
                    // weighting
                    // Use StringBuilder
                    String stopFlag = isStopWord ? "|stop=true" : ""; // add stop word flag if applicable
                    StringBuilder urlPartBuilder = new StringBuilder();
                    urlPartBuilder.append(e.getKey())
                            .append("|tf=").append(tf)
                            .append("|weightedTf=").append(formatDouble4Fast(weightedTf))
                            .append("|normalizedTf=").append(formatDouble6Fast(normalizedTf))
                            .append("|docLen=").append(docLen)
                            .append("|pos=").append(posStr)
                            .append("|weights=").append(weightStr)
                            .append(stopFlag);
                    urlParts.add(urlPartBuilder.toString());
                }
                // System.out.println("[Indexer] This is the indexed word: '" + word +
                // "' in " + sorted.size() + " URLs");

                // Calculate IDF which is log(totalDocs / docFreq)
                int docFreq = urlParts.size();
                double idf = (docFreq > 0 && totalDocs > 0) ? Math.log((double) totalDocs / docFreq) : 0.0;

                // document list with size-based limit
                int maxSize = isStopWord ? MAX_STRING_SIZE_STOP_WORD : MAX_STRING_SIZE;
                int estimatedSize = urlParts.size() * 150;
                StringBuilder docListBuilder = new StringBuilder(estimatedSize);
                int actualDocCount = 0;
                for (int i = 0; i < urlParts.size(); i++) {
                    String entry = urlParts.get(i);
                    int entrySize = entry.length() + (i > 0 ? 1 : 0);

                    // check if adding this entry would exceed size limit
                    if (docListBuilder.length() + entrySize > maxSize) {
                        System.out.println("[Indexer] Term '" + word + "' truncated at " + i
                                + " docs (size limit: " + docListBuilder.length() + " bytes)");
                        break;
                    }

                    if (i > 0)
                        docListBuilder.append(",");
                    docListBuilder.append(entry);
                    actualDocCount++;
                }

                // update docFreq to reflect actual count if truncated
                if (actualDocCount < docFreq) {
                    docFreq = actualDocCount;
                    idf = (docFreq > 0 && totalDocs > 0) ? Math.log((double) totalDocs / docFreq) : 0.0;
                }

                String docListStr = docListBuilder.toString();
                String finalValue = formatDouble8Fast(idf) + "\t" + docFreq + "\t" + docListStr;

                if (finalValue.length() > MAX_SAFE_STRING_SIZE) {
                    System.out.println("[Indexer] EMERGENCY: Term '" + word + "' string too large ("
                            + finalValue.length() + " bytes), skipping...");
                    return Collections.emptyList(); // Skip this term if it's still too large
                }

                return Collections.singletonList(new FlamePair(word, finalValue));
            });
            totalTfIdfTime += (System.currentTimeMillis() - t5);

            // -----------------------------------------------------------------------
            // Save index to pt-index
            // -----------------------------------------------------------------------

            long t6 = System.currentTimeMillis();

            // use Map for small entries, temp table for large ones
            String tempTableName = "pt-index-temp-bucket" + bucketNum + "-" + java.util.UUID.randomUUID().toString();
            FlamePairRDD existingPairs = null;
            Map<String, String> existingIndexEntriesForBucket = new ConcurrentHashMap<>();
            List<String> largeWords = new ArrayList<>();

            Set<String> wordsInBucket = new HashSet<>();
            List<FlamePair> preMergePairs = aggWordToUrlPosList.collect();
            for (FlamePair pair : preMergePairs) {
                wordsInBucket.add(pair._1());
            }

            if (!wordsInBucket.isEmpty()) {
                try {
                    List<String> wordList = new ArrayList<>(wordsInBucket);
                    Map<String, Row> rows = ctx.getKVS().getRows("pt-index", wordList);

                    int maxSizeForMemory = 10_000_000;
                    List<Map.Entry<String, String>> largeEntries = new ArrayList<>();

                    for (Map.Entry<String, Row> entry : rows.entrySet()) {
                        String word = entry.getKey();
                        Row r = entry.getValue();
                        if (r != null) {
                            String value = r.get("_value");
                            if (value == null || value.isEmpty()) {
                                for (String col : r.columns()) {
                                    if (col != null && !col.startsWith("_")) {
                                        value = r.get(col);
                                        if (value != null && !value.isEmpty())
                                            break;
                                    }
                                }
                            }
                            if (value != null && !value.isEmpty()) {
                                if (value.length() > maxSizeForMemory) {
                                    // Large entry - will write to temp table in parallel
                                    largeEntries.add(new java.util.AbstractMap.SimpleEntry<>(word, value));
                                    largeWords.add(word);
                                } else {
                                    // Small entry - keep in memory (fast)
                                    existingIndexEntriesForBucket.put(word, value);
                                }
                            }
                        }
                    }

                    // write large entries to temp table in parallel
                    if (!largeEntries.isEmpty()) {
                        ExecutorService writePool = Executors.newFixedThreadPool(16);
                        List<Future<?>> writeFutures = new ArrayList<>();

                        for (Map.Entry<String, String> largeEntry : largeEntries) {
                            writeFutures.add(writePool.submit(() -> {
                                try {
                                    Row tempRow = new Row(largeEntry.getKey());
                                    tempRow.put("value", largeEntry.getValue());
                                    ctx.getKVS().putRow(tempTableName, tempRow);
                                } catch (Exception e) {
                                    System.out.println("[WARNING] Failed to write large entry for '"
                                            + largeEntry.getKey() + "': " + e.getMessage());
                                }
                            }));
                        }

                        for (Future<?> f : writeFutures) {
                            try {
                                f.get();
                            } catch (Exception e) {
                                System.out.println("[WARNING] Error waiting for write: " + e.getMessage());
                            }
                        }
                        writePool.shutdown();
                    }
                } catch (Exception e) {
                    System.out.println("[WARNING] Failed to load existing index entries: " + e.getMessage());
                }
            }

            if (!largeWords.isEmpty()) {
                try {
                    existingPairs = ctx.fromTable(tempTableName, (Row row) -> {
                        String word = row.key();
                        String value = row.get("value");
                        if (value == null || value.isEmpty()) {
                            for (String col : row.columns()) {
                                if (col != null && !col.startsWith("_")) {
                                    value = row.get(col);
                                    if (value != null && !value.isEmpty())
                                        break;
                                }
                            }
                        }
                        return (value != null && !value.isEmpty()) ? word + "\t" + value : null;
                    }).mapToPair((String line) -> {
                        if (line == null)
                            return null;
                        int tabIdx = line.indexOf('\t');
                        if (tabIdx < 0)
                            return null;
                        String word = line.substring(0, tabIdx);
                        String value = line.substring(tabIdx + 1);
                        return new FlamePair(word, value);
                    });
                } catch (Exception e) {
                    System.out.println("[WARNING] Failed to create existing pairs RDD: " + e.getMessage());
                    existingPairs = ctx.parallelize(Collections.emptyList()).mapToPair((String s) -> null);
                }
            } else {
                existingPairs = ctx.parallelize(Collections.emptyList()).mapToPair((String s) -> null);
            }

            // use cogroup to merge new and existing entries
            // small entries will be checked from Map
            final FlamePairRDD finalExistingPairs = existingPairs;

            FlamePairRDD cogrouped = aggWordToUrlPosList.cogroup(finalExistingPairs);

            // parse cogroup output and merge
            FlamePairRDD mergedIndex = cogrouped.flatMapToPair(pair -> {
                String word = pair._1();
                String cogroupValue = pair._2();

                if (cogroupValue == null || cogroupValue.isEmpty()) {
                    return Collections.emptyList();
                }

                int bracket1End = cogroupValue.indexOf(']');
                if (bracket1End < 0) {
                    return Collections.emptyList();
                }

                String newValuesStr = cogroupValue.substring(1, bracket1End);
                int bracket2Start = cogroupValue.indexOf('[', bracket1End + 1);
                String oldValuesStr = "";
                if (bracket2Start >= 0) {
                    int bracket2End = cogroupValue.lastIndexOf(']');
                    if (bracket2End > bracket2Start) {
                        oldValuesStr = cogroupValue.substring(bracket2Start + 1, bracket2End);
                    }
                }

                String newValue = newValuesStr.isEmpty() ? null : newValuesStr;

                String existingValue = oldValuesStr.isEmpty() ? null : oldValuesStr;

                if (newValue == null || newValue.isEmpty()) {
                    return Collections.emptyList();
                }

                int tab1 = newValue.indexOf('\t');
                if (tab1 < 0) {
                    if (existingValue == null || existingValue.isEmpty()) {
                        return Collections.singletonList(new FlamePair(word, newValue));
                    }
                    return Collections.singletonList(new FlamePair(word, newValue));
                }
                int tab2 = newValue.indexOf('\t', tab1 + 1);
                if (tab2 < 0) {
                    if (existingValue == null || existingValue.isEmpty()) {
                        return Collections.singletonList(new FlamePair(word, newValue));
                    }
                    return Collections.singletonList(new FlamePair(word, newValue));
                }

                String newDocList = newValue.substring(tab2 + 1);
                String newDocFreqStr = newValue.substring(tab1 + 1, tab2);

                if (existingValue == null || existingValue.isEmpty()) {
                    int newDocFreq = Integer.parseInt(newDocFreqStr);
                    double mergedIdf = (newDocFreq > 0 && totalDocs > 0) ? Math.log((double) totalDocs / newDocFreq)
                            : 0.0;
                    String mergedValue = formatDouble8Fast(mergedIdf) + "\t" + newDocFreq + "\t" + newDocList;
                    return Collections.singletonList(new FlamePair(word, mergedValue));
                }

                // Parse existing value without split
                int existingTab1 = existingValue.indexOf('\t');
                if (existingTab1 < 0) {
                    int newDocFreq = Integer.parseInt(newDocFreqStr);
                    double mergedIdf = (newDocFreq > 0 && totalDocs > 0) ? Math.log((double) totalDocs / newDocFreq)
                            : 0.0;
                    String mergedValue = formatDouble8Fast(mergedIdf) + "\t" + newDocFreq + "\t" + newDocList;
                    return Collections.singletonList(new FlamePair(word, mergedValue));
                }
                int existingTab2 = existingValue.indexOf('\t', existingTab1 + 1);
                if (existingTab2 < 0) {
                    int newDocFreq = Integer.parseInt(newDocFreqStr);
                    double mergedIdf = (newDocFreq > 0 && totalDocs > 0) ? Math.log((double) totalDocs / newDocFreq)
                            : 0.0;
                    String mergedValue = formatDouble8Fast(mergedIdf) + "\t" + newDocFreq + "\t" + newDocList;
                    return Collections.singletonList(new FlamePair(word, mergedValue));
                }

                String existingDocList = existingValue.substring(existingTab2 + 1);

                Set<String> existingUrls = new HashSet<>();
                // Parse existing doc list without split
                int docStart = 0;
                while (docStart < existingDocList.length()) {
                    int docEnd = existingDocList.indexOf(',', docStart);
                    String docEntry = (docEnd == -1) ? existingDocList.substring(docStart)
                            : existingDocList.substring(docStart, docEnd);
                    docEntry = docEntry.trim();
                    if (!docEntry.isEmpty()) {
                        int urlEnd = docEntry.indexOf("|");
                        if (urlEnd > 0) {
                            existingUrls.add(docEntry.substring(0, urlEnd));
                        }
                    }
                    docStart = (docEnd == -1) ? existingDocList.length() : docEnd + 1;
                }

                List<String> mergedDocs = new ArrayList<>();
                final boolean isStopWordMerged = stopWords.contains(word.toLowerCase());
                int maxMergedDocs = isStopWordMerged ? MAX_DOCS_PER_STOP_WORD : MAX_DOCS_PER_TERM;

                // limit check to prevent intermediate growing too large
                docStart = 0;
                while (docStart < existingDocList.length() && mergedDocs.size() < maxMergedDocs) {
                    int docEnd = existingDocList.indexOf(',', docStart);
                    String docEntry = (docEnd == -1) ? existingDocList.substring(docStart)
                            : existingDocList.substring(docStart, docEnd);
                    docEntry = docEntry.trim();
                    if (!docEntry.isEmpty()) {
                        mergedDocs.add(docEntry);
                    }
                    docStart = (docEnd == -1) ? existingDocList.length() : docEnd + 1;
                }

                docStart = 0;
                while (docStart < newDocList.length() && mergedDocs.size() < maxMergedDocs) {
                    int docEnd = newDocList.indexOf(',', docStart);
                    String docEntry = (docEnd == -1) ? newDocList.substring(docStart)
                            : newDocList.substring(docStart, docEnd);
                    docEntry = docEntry.trim();
                    if (!docEntry.isEmpty()) {
                        int urlEnd = docEntry.indexOf("|");
                        if (urlEnd > 0) {
                            String url = docEntry.substring(0, urlEnd);
                            if (!existingUrls.contains(url)) {
                                mergedDocs.add(docEntry);
                                existingUrls.add(url);
                            }
                        } else {
                            mergedDocs.add(docEntry);
                        }
                    }
                    docStart = (docEnd == -1) ? newDocList.length() : docEnd + 1;
                }

                if (mergedDocs.size() >= maxMergedDocs) {
                    System.out.println("[Indexer] Merged term '" + word + "' limited to " + maxMergedDocs
                            + " docs (count limit reached during merge)");
                }

                // sort merged docs by normalizedTf (highest weight first)
                // so truncation will keeps the most important entries
                mergedDocs.sort((e1, e2) -> {
                    double tf1 = 0.0, tf2 = 0.0;
                    int tfStart1 = e1.indexOf("|normalizedTf=");
                    if (tfStart1 >= 0) {
                        int tfEnd1 = e1.indexOf("|", tfStart1 + 14);
                        if (tfEnd1 < 0)
                            tfEnd1 = e1.length();
                        try {
                            tf1 = Double.parseDouble(e1.substring(tfStart1 + 14, tfEnd1));
                        } catch (NumberFormatException ignored) {
                        }
                    }
                    int tfStart2 = e2.indexOf("|normalizedTf=");
                    if (tfStart2 >= 0) {
                        int tfEnd2 = e2.indexOf("|", tfStart2 + 14);
                        if (tfEnd2 < 0)
                            tfEnd2 = e2.length();
                        try {
                            tf2 = Double.parseDouble(e2.substring(tfStart2 + 14, tfEnd2));
                        } catch (NumberFormatException ignored) {
                        }
                    }
                    return Double.compare(tf2, tf1);
                });

                int mergedDocFreq = mergedDocs.size();
                double mergedIdf = (mergedDocFreq > 0 && totalDocs > 0) ? Math.log((double) totalDocs / mergedDocFreq)
                        : 0.0;

                // use smaller limit for stop words in merge section too
                int maxMergedSize = isStopWordMerged ? MAX_STRING_SIZE_STOP_WORD : MAX_STRING_SIZE;
                int estimatedSize = mergedDocs.size() * 150;
                StringBuilder mergedDocListBuilder = new StringBuilder(estimatedSize);
                int actualMergedCount = 0;
                for (int i = 0; i < mergedDocs.size(); i++) {
                    String entry = mergedDocs.get(i);
                    int entrySize = entry.length() + (i > 0 ? 1 : 0);

                    if (mergedDocListBuilder.length() + entrySize > maxMergedSize) {
                        System.out.println("[Indexer] Merged term '" + word + "' truncated at " + i
                                + " docs (size limit: " + mergedDocListBuilder.length() + " bytes)");
                        break;
                    }

                    if (i > 0)
                        mergedDocListBuilder.append(",");
                    mergedDocListBuilder.append(entry);
                    actualMergedCount++;
                }

                // update mergedDocFreq to reflect actual count if truncated
                if (actualMergedCount < mergedDocFreq) {
                    mergedDocFreq = actualMergedCount;
                    mergedIdf = (mergedDocFreq > 0 && totalDocs > 0) ? Math.log((double) totalDocs / mergedDocFreq)
                            : 0.0;
                }

                String mergedDocListStr = mergedDocListBuilder.toString();

                if (mergedDocListStr.length() > MAX_SAFE_STRING_SIZE) {
                    System.out.println("[Indexer] EMERGENCY: Term '" + word + "' merged string too large ("
                            + mergedDocListStr.length() + " bytes), truncating...");
                    int targetLength = MAX_SAFE_STRING_SIZE - 1000;
                    int lastComma = mergedDocListStr.lastIndexOf(',', targetLength);
                    if (lastComma > 0) {
                        mergedDocListStr = mergedDocListStr.substring(0, lastComma);
                        mergedDocFreq = mergedDocListStr.isEmpty() ? 0 : mergedDocListStr.split(",").length;
                        mergedIdf = (mergedDocFreq > 0 && totalDocs > 0) ? Math.log((double) totalDocs / mergedDocFreq)
                                : 0.0;
                    } else {
                        System.out.println(
                                "[Indexer] EMERGENCY: Cannot truncate term '" + word + "', skipping...");
                        return Collections.emptyList();
                    }
                }

                String mergedValue = formatDouble8Fast(mergedIdf) + "\t" + mergedDocFreq + "\t" + mergedDocListStr;

                if (mergedValue.length() > MAX_SAFE_STRING_SIZE) {
                    System.out.println("[Indexer] EMERGENCY: Term '" + word + "' final value too large ("
                            + mergedValue.length() + " bytes), skipping...");
                    return Collections.emptyList();
                }

                return Collections.singletonList(new FlamePair(word, mergedValue));
            });

            mergedIndex.saveAsTable("pt-index");

            try {
                if (existingPairs != null) {
                    existingPairs.destroy();
                }
                ctx.getKVS().delete(tempTableName);
            } catch (Exception e) {
                System.out.println("[WARNING] Failed to clean up temp table " + tempTableName + ": " + e.getMessage());
            }

            for (String record : finalDocLengthList) {

                if (record == null || record.isEmpty())
                    continue;

                // url, length, maxTf, title, preview
                // Use indexOf instead of split for better performance
                int comma1 = record.indexOf(',');
                if (comma1 < 0)
                    continue;
                int comma2 = record.indexOf(',', comma1 + 1);
                if (comma2 < 0)
                    continue;
                int comma3 = record.indexOf(',', comma2 + 1);
                int comma4 = (comma3 > 0) ? record.indexOf(',', comma3 + 1) : -1;

                String url = record.substring(0, comma1);
                String lenStr = record.substring(comma1 + 1, comma2);
                String maxTfStr = record.substring(comma2 + 1, comma3 > 0 ? comma3 : record.length());
                String title = (comma3 > 0 && comma4 > 0) ? record.substring(comma3 + 1, comma4)
                        : (comma3 > 0 ? record.substring(comma3 + 1) : "");
                // String preview = parts.length > 4 ? parts[4] : "";

                // ensure url blongs to current bucket
                try {
                    int urlHash = Math.abs(url.hashCode());
                    int urlBucket = urlHash % FINAL_NUM_HASH_BUCKETS;
                    if (urlBucket != bucketNum) {
                        continue;
                    }
                } catch (Exception e) {
                    continue;
                }

                String hashed = finalUrlHashCache.computeIfAbsent(url, k -> {
                    try {
                        String hashResult = Hasher.hash(k);
                        int newlineIdx = hashResult.indexOf('\n');
                        return (newlineIdx > 0) ? hashResult.substring(0, newlineIdx) : hashResult;
                    } catch (Exception ex) {
                        return Hasher.hash(k); // Fallback
                    }
                });

                // skip if this URL was already saved in this bucket (prevent duplicates)
                // Use add, returns false if already present
                if (!savedUrlsInBucket.add(hashed)) {
                    continue;
                }

                Row docRow = new Row(hashed);

                // add/update columns
                docRow.put("url", url);
                docRow.put("length", lenStr);
                docRow.put("maxTf", maxTfStr);
                docRow.put("title", title);

                ctx.getKVS().putRow(CHECKPOINT_TABLE, docRow);
            }
            // checkpoint every 5 buckets
            if (bucketNum % 5 == 0 || bucketNum == FINAL_NUM_HASH_BUCKETS - 1) {
                Row bucketCheckpoint = new Row("bucket" + bucketNum);
                bucketCheckpoint.put("bucket", Integer.toString(bucketNum));
                ctx.getKVS().putRow(BATCH_CHECKPOINT_TABLE, bucketCheckpoint);
            }
            totalSaveTime += (System.currentTimeMillis() - t6);

            // URLs don't repeat across buckets, so cache can be safely cleared
            urlHashCache.clear();

        }

        // Print all timer results once at the end
        ctx.output("[TIMER] flatMapToPair(word→url:pos) done in " + totalFlatMapTime + " ms\n");
        ctx.output("[TIMER] foldByKey(merge positions) done in " + totalFoldByKeyTime + " ms\n");
        ctx.output("[TIMER] flatMapToPair(TF/IDF + sorting) done in " + totalTfIdfTime + " ms\n");
        ctx.output("[TIMER] saveAsTable done in " + totalSaveTime + " ms\n");
        ctx.output("[TIMER] TOTAL runtime: " + (System.currentTimeMillis() - totalStart) + " ms\n");
    }

    /**
     * Tokenize page content
     * 
     * @param page page content as a string
     * @return returns list of tokens
     * @author Aaron Alakkadan and refactored by Evan Law
     */
    private static List<String> tokenize(String page) {
        String noHtml = HTML_TAGS.matcher(page).replaceAll(" ");
        String cleaned = NON_ALPHANUMERIC.matcher(noHtml).replaceAll(" ").toLowerCase();
        String[] toks = WHITESPACE.split(cleaned);
        List<String> tokens = new ArrayList<>();
        for (String tok : toks) { // trim and filter out empty tokens

            if (tok == null)
                continue; /// JAKE: adding null check

            String trimmed = tok.trim();
            if (!trimmed.isEmpty() && isWorthIndexing(trimmed)) {
                tokens.add(trimmed);
            }
        }
        return tokens;
    }

    /**
     * Tokenize page content and return tokens with visibility flags
     * Visible text weight 1.00. Metadata weighted 0.005
     * 
     * @param page page content as string
     * @return returns list of token weight pairs as String "token|weight"
     * @author Evan Law
     */
    private static List<String> tokenizeWithWeights(String page) {
        List<String> tokens = new ArrayList<>();

        // Extract visible text by removing scripts, styles, and comments first
        String visibleText = page;

        // Remove script and style tags and their content
        visibleText = SCRIPT_TAG.matcher(visibleText).replaceAll(" ");
        visibleText = STYLE_TAG.matcher(visibleText).replaceAll(" ");

        // Remove HTML comments and meta tags
        visibleText = HTML_COMMENT.matcher(visibleText).replaceAll(" ");
        visibleText = META_TAG.matcher(visibleText).replaceAll(" ");

        // Remove JSON-like structures in script type attributes or data attributes as a
        // heuristic
        visibleText = SCRIPT_JSON.matcher(visibleText).replaceAll(" ");
        visibleText = SCRIPT_JSON_LD.matcher(visibleText).replaceAll(" ");

        // Tokenize visible text with weight 1.0 for visible content
        String visibleNoHtml = HTML_TAGS.matcher(visibleText).replaceAll(" ");
        String visibleCleaned = NON_ALPHANUMERIC.matcher(visibleNoHtml).replaceAll(" ").toLowerCase();
        String[] visibleToks = WHITESPACE.split(visibleCleaned);
        for (String tok : visibleToks) {
            if (tok == null)
                continue;
            String trimmed = tok.trim();
            if (!trimmed.isEmpty()) {
                tokens.add(trimmed + "|1.0");
            }
        }

        // Extract metadata (scripts, styles, JSON,...) with lower weight 0.005
        String metadataText = page;

        // Extract script tag contents
        java.util.regex.Matcher scriptMatcher = SCRIPT_CONTENT.matcher(metadataText);
        while (scriptMatcher.find()) {
            String scriptContent = scriptMatcher.group(1);
            String metaNoHtml = HTML_TAGS.matcher(scriptContent).replaceAll(" ");
            String metaCleaned = NON_ALPHANUMERIC.matcher(metaNoHtml).replaceAll(" ").toLowerCase();
            String[] metaToks = WHITESPACE.split(metaCleaned);
            for (String tok : metaToks) {
                if (tok == null)
                    continue;
                String trimmed = tok.trim();
                if (trimmed.length() >= 2 && !trimmed.isEmpty()) {
                    tokens.add(trimmed + "|0.001"); // weight 0.005 for metadata
                }
            }
        }

        // Extract style tag contents
        java.util.regex.Matcher styleMatcher = STYLE_CONTENT.matcher(metadataText);
        while (styleMatcher.find()) {
            String styleContent = styleMatcher.group(1);
            String metaNoHtml = HTML_TAGS.matcher(styleContent).replaceAll(" ");
            String metaCleaned = NON_ALPHANUMERIC.matcher(metaNoHtml).replaceAll(" ").toLowerCase();
            String[] metaToks = WHITESPACE.split(metaCleaned);
            for (String tok : metaToks) {
                if (tok == null)
                    continue;
                String trimmed = tok.trim();
                if (trimmed.length() >= 2 && !trimmed.isEmpty()) {
                    tokens.add(trimmed + "|0.001"); // Weight 0.005 for metadata
                }
            }
        }

        return tokens;
    }

    /**
     * loads stop words from the stopwords.txt file
     * 
     * @return returns a set of stop words
     * @author Evan Law
     */
    private static Set<String> loadStopWords() {
        Set<String> stopWords = new HashSet<>();
        String resourcePath = "cis5550/resources/stopwords.txt";
        try (InputStream in = Indexer.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (in == null) {
                System.err.println("[Aaron’s Indexer] Stopword file not found at " + resourcePath);
                return stopWords;
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) { // read each line of the file
                    String trimmed = line.trim().toLowerCase(Locale.US);
                    if (!trimmed.isEmpty() && !trimmed.startsWith("#")) { // skip empty lines and starting with # aka
                                                                          // comments
                        stopWords.add(trimmed);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("[Aaron’s Indexer] Failed to load stopwords: " + e.getMessage());
        }
        System.out.println("[Aaron’s Indexer] Loaded " + stopWords.size() + " stop words.");
        return stopWords; // return the set of stop words
    }

    private static String sanitizeMetadata(String s, int maxLen) {

        if (s == null)
            return "";
        String cleaned = s.replaceAll("[\\r\\n\\t]+", " "); // remove new lines and tabs
        cleaned = cleaned.replace(',', ' '); // remove commas (using commas as a delim)
        cleaned = cleaned.replaceAll("\\s+", " ").trim(); // strip whitespace
        if (cleaned.length() > maxLen) {
            cleaned = cleaned.substring(0, maxLen);
        }

        return cleaned;
    }

    private static String extractTitle(String page) {

        if (page == null)
            return "";
        java.util.regex.Matcher m = TITLE_TAG.matcher(page);
        String title = "";
        if (m.find()) {
            title = m.group(1);
        }

        title = HTML_TAGS.matcher(title).replaceAll(" ");
        return sanitizeMetadata(title, 200);
    }

    private static boolean isWorthIndexing(String s) {
        if (s == null)
            return false;
        s = s.toLowerCase();

        int len = s.length();
        if (len < 2 || len > 20)
            return false;

        if (s.matches("^[a-z]+$"))
            return true; // normal word
        if (s.matches("^\\d{2,4}$"))
            return true; // 2–4 digit numbers (007, 404, 2024)

        return false;
    }

}
