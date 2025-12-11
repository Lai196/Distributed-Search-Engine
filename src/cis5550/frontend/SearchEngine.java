package cis5550.frontend;

import cis5550.kvs.*;
import cis5550.tools.Hasher;
import opennlp.tools.stemmer.PorterStemmer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import java.util.*;
import java.io.*;
import java.nio.charset.StandardCharsets;

public class SearchEngine {
    // Cache stopwords to avoid expensive KVS lookups for common stopword queries
    private static final Set<String> stopwordsCache = loadStopWords();
    private static final List<String> allTerms = new ArrayList<>();
    private static boolean termsLoaded = false;
    // Map from stem to list of words that stem to it (for finding related word forms)
    private static final Map<String, Set<String>> stemToWords = new HashMap<>();
    
    // Cache search results to avoid repeated queries
    private static final Map<String, List<SearchResult>> searchResultCache = new HashMap<>();
    private static final int MAX_CACHE_SIZE = 1000;  // Limit cache size to prevent memory issues


    // REGEX patterns for filtering page previews
    private static final Pattern SCRIPT_STYLE = Pattern.compile("(?is)<(script|style)[^>]*?>[\\s\\S]*?</\\1>");
    private static final Pattern COMMENTS = Pattern.compile("(?is)<!--.*?-->");
    private static final Pattern TAGS = Pattern.compile("<[^>]+>");
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");
    
    /**
     * Get the total number of cached results for a query
     * Returns 0 if not cached
     */
    public static int getCachedResultCount(String query) {
        String cacheKey = query.toLowerCase().trim();
        synchronized (searchResultCache) {
            List<SearchResult> cached = searchResultCache.get(cacheKey);
            return (cached != null) ? cached.size() : 0;
        }
    }

    public static class SearchResult {
        public final String url;
        public final double totalScore;
        public final double tfidfScore;
        public final double pageRankScore;
        public final String title;
        public final String preview;

        public SearchResult(String url, double totalScore, double tfidfScore, double pageRankScore, String title, String preview){
            this.url = url;
            this.totalScore = totalScore;
            this.tfidfScore = tfidfScore;
            this.pageRankScore = pageRankScore;
            this.title = title;
            this.preview = preview;
        }
        
    }


    public static List<SearchResult> executeSearch(KVSClient kvs, String query, int limit) throws Exception {
        return executeSearch(kvs, query, limit, 1, 15);
    }
    
    public static List<SearchResult> executeSearch(KVSClient kvs, String query, int limit, int page, int pageSize) throws Exception {
        // Normalize query for cache key (lowercase, trimmed)
        String cacheKey = query.toLowerCase().trim();
        
        // Default to page 1 if invalid
        if (page < 1) page = 1;
        
        // Check cache first for previous searches
        synchronized (searchResultCache) {
            List<SearchResult> cached = searchResultCache.get(cacheKey);
            if (cached != null) {
                // Return paginated results from cache
                int totalResults = cached.size();
                int startIndex = (page - 1) * pageSize;
                int endIndex = Math.min(startIndex + pageSize, totalResults);
                
                System.out.println("[CACHE HIT] Query: \"" + query + "\" - Page " + page + ", pageSize " + pageSize + ", startIndex " + startIndex + ", endIndex " + endIndex + ", totalResults " + totalResults);
                
                List<SearchResult> pageResults;
                if (startIndex >= totalResults) {
                    // Page beyond available results
                    pageResults = new ArrayList<>();
                } else {
                    pageResults = new ArrayList<>(cached.subList(startIndex, endIndex));
                }
                
                System.out.println("[CACHE HIT] Query: \"" + query + "\" - Returning page " + page + " (" + pageResults.size() + " of " + totalResults + " cached results, indices " + startIndex + "-" + (endIndex - 1) + ")");
                return pageResults;
            }
        }
        System.out.println("[CACHE MISS] Query: \"" + query + "\" - Computing results...");

        List<SearchResult> results = new ArrayList<>();               // initialize arraylist of SearchResults to hold output

        List<String> searchTerms = tokenizeUserSearch(query);         // tokenize the user's search & check if empty
        if (searchTerms.isEmpty()) {
            // Cache empty results
            synchronized (searchResultCache) {
                if (searchResultCache.size() >= MAX_CACHE_SIZE) {
                    String firstKey = searchResultCache.keySet().iterator().next();
                    searchResultCache.remove(firstKey);
                }
                searchResultCache.put(cacheKey, new ArrayList<>());
            }
            return results;
        }
        Map<String, Double> tfidfSums = new HashMap<>();

        // If single-term query is a stop word, return empty results immediately to prevent unnecessary KVS lookups
        if (searchTerms.size() == 1 && stopwordsCache.contains(searchTerms.get(0).toLowerCase())) {
            // Cache stopword results
            synchronized (searchResultCache) {
                if (searchResultCache.size() >= MAX_CACHE_SIZE) {
                    String firstKey = searchResultCache.keySet().iterator().next();
                    searchResultCache.remove(firstKey);
                }
                searchResultCache.put(cacheKey, new ArrayList<>());
            }
            return results;
        }

        // Expand search terms to include stemmed versions (to match Indexer behavior)
        List<String> expandedTerms = new ArrayList<>();
        PorterStemmer stemmer = new PorterStemmer();
        for (String term : searchTerms) {
            String termLower = term.toLowerCase();
            expandedTerms.add(termLower); // Always include original term (lowercased for consistency)
            
            String stemmed = stemmer.stem(termLower);
            if (stemmed != null && !stemmed.isEmpty() && !stemmed.equals(termLower) && !stopwordsCache.contains(termLower)) {
                expandedTerms.add(stemmed); // Also include stemmed version if different
                
                // look up other words that stem to the same thing
                synchronized (stemToWords) {
                    Set<String> relatedWords = stemToWords.get(stemmed);
                    if (relatedWords != null) {
                        for (String relatedWord : relatedWords) {
                            // Only add if it's different from what we already have
                            if (!relatedWord.equals(termLower) && !relatedWord.equals(stemmed)) {
                                expandedTerms.add(relatedWord);
                            }
                        }
                    }
                }
            }
        }

        // Store positions for each term to enable phrase search
        Map<String, Map<String, List<Integer>>> positionsByTerm = new HashMap<>();

        // Track which original query term each expanded term corresponds to
        Map<String, String> expandedToOriginal = new HashMap<>();
        for (int i = 0; i < searchTerms.size(); i++) {
            String originalTerm = searchTerms.get(i);
            String originalTermLower = originalTerm.toLowerCase();
            expandedToOriginal.put(originalTermLower, originalTerm);
            
            String stemmed = stemmer.stem(originalTermLower);
            if (stemmed != null && !stemmed.isEmpty() && !stemmed.equals(originalTermLower) && !stopwordsCache.contains(originalTermLower)) {
                expandedToOriginal.put(stemmed, originalTerm);
                
                // Map related words back to the original query term
                synchronized (stemToWords) {
                    Set<String> relatedWords = stemToWords.get(stemmed);
                    if (relatedWords != null) {
                        for (String relatedWord : relatedWords) {
                            if (!relatedWord.equals(originalTermLower) && !relatedWord.equals(stemmed)) {
                                expandedToOriginal.put(relatedWord, originalTerm);
                            }
                        }
                    }
                }
            }
        }

        // Track which URLs we've already processed for each original query term
        // This prevents double-counting TF-IDF when both original and stemmed versions match the same document
        Map<String, Set<String>> processedUrlsPerOriginalTerm = new HashMap<>();
        for (String originalTerm : searchTerms) {
            processedUrlsPerOriginalTerm.put(originalTerm, new HashSet<>());
        }

        // pre-check if any terms exist in index before processing
        boolean hasAnyMatches = false;
        for (String term : expandedTerms) {
            Row testRow = kvs.getRow("pt-index", term);
            if (testRow != null) {
                hasAnyMatches = true;
                break;
            }
        }
        
        // Early termination if no terms match, return empty results immediately
        if (!hasAnyMatches) {
            return results;
        }

        for (String term : expandedTerms){    // for each term in user's search (including stems) -----------------------------------------------------------------------

            Row row = kvs.getRow("pt-index", term);        // check if term is in pt-index
            if (row == null) continue;                               // skip to next term if not

//            String idfString = row.get("idf");                  // get the idf score and docList from the row
            String rawValue = row.get("_value");            // all value data is stored in the _value column of term
            if (rawValue == null || rawValue.isEmpty()) {
                for (String col : row.columns()) {
                    if (col == null || col.startsWith("_")) {  // skip metadata columns as we just want the value columns
                        continue;
                    }
                    String val = row.get(col);
                    if (val != null && !val.isEmpty()) {  // take the first non-empty column value as the rawValue
                        rawValue = val;
                        break;
                    }
                }
            }
            if (rawValue == null || rawValue.isEmpty()) {
                continue;
            }
            
            int firstTab = rawValue.indexOf('\t');
            if (firstTab < 0) continue;
            
            int secondTab = rawValue.indexOf('\t', firstTab + 1);
            if (secondTab < 0) continue;
            
            String idfString = rawValue.substring(0, firstTab);
            String docList = rawValue.substring(secondTab + 1);

            if (idfString.isEmpty() || docList.isEmpty()) continue;  // skip to next term if either is missing

            //---------------------------------------------------
            // Get idf score
            //---------------------------------------------------

            double idf = 0.0;
            try {
                idf = Double.parseDouble(idfString);                // try parsing the idf string to a double
            } catch (NumberFormatException nfe) {
                System.out.println("ERROR PARSING IDF SCORE TO DOUBLE for term: " + term);
            }

            //--------------------------------------------------------------------------------------------------------------------------
            // Get tfidf score for each url that contained user's term & add to running tfidf total for that url
            // 
            //  docList example: foo.com|tf=3|normalizedTf=0.012345|docLen=243|pos=1 19 98, example.com|tf=2|normalizedTf=0.0532|docLen=200|pos=3 53
            //--------------------------------------------------------------------------------------------------------------------------

            Map<String, List<Integer>> urlToPositions = new HashMap<>(); // For phrase detection
            
            int docStart = 0;
            while (docStart < docList.length()) {
                int docEnd = docList.indexOf(',', docStart);
                if (docEnd < 0) docEnd = docList.length();
                
                String doc = docList.substring(docStart, docEnd).trim();
                docStart = docEnd + 1;
                
                if (doc.isEmpty()) continue;

                int urlSplit = doc.indexOf("|");
                if (urlSplit < 0) continue;
                String url = doc.substring(0, urlSplit);              // store url
                String urlAttributes = doc.substring(urlSplit+1);

                boolean isStopWordEntry = false;
                double normalizedTf = 0.0;
                
                int attrStart = 0;
                while (attrStart < urlAttributes.length()) {
                    int attrEnd = urlAttributes.indexOf('|', attrStart);
                    if (attrEnd < 0) attrEnd = urlAttributes.length();
                    
                    String part = urlAttributes.substring(attrStart, attrEnd).trim();
                    attrStart = attrEnd + 1;
                    
                    if (part.startsWith("normalizedTf=")) {
                        String normTfString = part.substring("normalizedTf=".length());
                        try {
                            normalizedTf = Double.parseDouble(normTfString);
                        } catch (NumberFormatException nfe){
                            System.out.println("ERROR PARSING normalizedTf TO DOUBLE for term: " + term);
                        }
                    } else if (part.startsWith("pos=")) {
                        String posStr = part.substring("pos=".length());
                        List<Integer> posList = new ArrayList<>();
                        int posStart = 0;
                        while (posStart < posStr.length()) {
                            while (posStart < posStr.length() && posStr.charAt(posStart) == ' ') posStart++;
                            if (posStart >= posStr.length()) break;
                            
                            int posEnd = posStart;
                            while (posEnd < posStr.length() && posStr.charAt(posEnd) != ' ') posEnd++;
                            
                            try {
                                posList.add(Integer.parseInt(posStr.substring(posStart, posEnd)));
                            } catch (Exception ignore) {}
                            posStart = posEnd + 1;
                        }
                        urlToPositions.put(url, posList);
                    } else if (part.startsWith("stop=")) {
                        String stopValue = part.substring("stop=".length());
                        isStopWordEntry = "true".equalsIgnoreCase(stopValue);
                    }
                }

                if (normalizedTf <= 0.0 || isStopWordEntry) continue;            // if the normalizedTf score is 0 or less, it will have a final score of 0 so ignore or is a stop word

                // Only add TF-IDF if we haven't already processed this URL for the original query term
                // This prevents double-counting when both original and stemmed versions match the same document
                String originalTerm = expandedToOriginal.get(term);
                Set<String> processedUrls = processedUrlsPerOriginalTerm.get(originalTerm);
                boolean alreadyProcessed = (processedUrls != null && processedUrls.contains(url));
                
                if (!alreadyProcessed) {
                    if (processedUrls != null) {
                        processedUrls.add(url); // Mark this URL as processed for this original term
                    }

                    double tfidf = normalizedTf * idf;            // otherwise calculate the tfidf score to be added to given url for having user's term

                    tfidfSums.merge(url, tfidf, (prevScore, increm) -> prevScore + increm);   // add the score to the url's overall summed tfidf score
                }
                // Always collect positions for phrase matching, even if we've already processed this URL for TF-IDF to ensure phrase search works correctly with stemmed terms

            } // end of for each "url|tf|normTf|docLen|pos" -----------------------------------------------------------------------------------------------------

            // Merge position data: if this is a stemmed term, merge positions with the original term
            // This must happen even if we skipped TF-IDF to ensure phrase search has all position data
            String originalTerm = expandedToOriginal.get(term);
            Map<String, List<Integer>> existingPositions = positionsByTerm.get(originalTerm);
            if (existingPositions == null) {
                positionsByTerm.put(originalTerm, new HashMap<>(urlToPositions));
            } else {
                // Merge positions: combine position lists for URLs that appear in both
                for (Map.Entry<String, List<Integer>> entry : urlToPositions.entrySet()) {
                    String url = entry.getKey();
                    List<Integer> newPositions = entry.getValue();
                    List<Integer> existingPos = existingPositions.get(url);
                    if (existingPos == null) {
                        existingPositions.put(url, new ArrayList<>(newPositions));
                    } else {
                        // Merge and deduplicate positions
                        Set<Integer> posSet = new HashSet<>(existingPos);
                        posSet.addAll(newPositions);
                        List<Integer> merged = new ArrayList<>(posSet);
                        Collections.sort(merged);
                        existingPositions.put(url, merged);
                    }
                }
            }
        } // end of for each term in user's search --------------------------------------------------------------------------------------------------------------


        //------------------------------------------------------
        // Phrase Search Boost (NEW FEATURE EC)
        //------------------------------------------------------
        boolean hasPhrase = searchTerms.size() > 1;
        if (hasPhrase) {
            Set<String> urlsToRemove = new HashSet<>();
            for (String url : new ArrayList<>(tfidfSums.keySet())) {
                // First, check if ALL terms appear in this URL
                boolean allTermsPresent = true;
                for (String term : searchTerms) {
                    String termLower = term.toLowerCase();
                    Map<String, List<Integer>> posMap = positionsByTerm.get(termLower);
                    if (posMap == null) {
                        allTermsPresent = false;
                        break;
                    }
                    List<Integer> positions = posMap.get(url);
                    if (positions == null || positions.isEmpty()) {
                        allTermsPresent = false;
                        break;
                    }
                }
                
                // If not all terms are present, remove this URL
                if (!allTermsPresent) {
                    urlsToRemove.add(url);
                    continue;
                }
                
                // Check if all consecutive words appear adjacently (for phrase boost)
                boolean phraseFound = true;
                for (int i = 0; i < searchTerms.size() - 1; i++) {
                    String w1 = searchTerms.get(i).toLowerCase();
                    String w2 = searchTerms.get(i + 1).toLowerCase();
                    Map<String, List<Integer>> pos1Map = positionsByTerm.get(w1);
                    Map<String, List<Integer>> pos2Map = positionsByTerm.get(w2);

                    if (pos1Map == null || pos2Map == null) {
                        phraseFound = false;
                        break;
                    }

                    List<Integer> pos1 = pos1Map.get(url);
                    List<Integer> pos2 = pos2Map.get(url);

                    if (pos1 == null || pos1.isEmpty() || pos2 == null || pos2.isEmpty()) {
                        phraseFound = false;
                        break;
                    }

                    // Check adjacency: w2 must appear immediately after w1
                    boolean adjacent = false;
                    for (int p1 : pos1) {
                        if (pos2.contains(p1 + 1)) {
                            adjacent = true;
                            break;
                        }
                    }

                    if (!adjacent) {
                        phraseFound = false;
                        break;
                    }
                }

                if (phraseFound) {
                    // Boost results that have the exact phrase
                    double boosted = tfidfSums.get(url) * 1.5; // 50% boost for phrase match
                    tfidfSums.put(url, boosted);
                }
                // If phrase not found but all terms are present, keep the result but don't boost
            }
            
            // Remove URLs that don't contain all terms
            for (String url : urlsToRemove) {
                tfidfSums.remove(url);
            }
        }


            //------------------------------------------------------
            // Lookup & combine pagerank score with tf-idf score
            // Batch KVS lookups to reduce network round trips
            //------------------------------------------------------

            // Pre-compute all hashed URLs and cache them
            Map<String, String> urlHashCache = new HashMap<>();
            for (String url : tfidfSums.keySet()) {
                try {
                    String hashedURL = Hasher.hash(url).split("\n")[0];
                    urlHashCache.put(url, hashedURL);
                } catch (Exception e) {
                    System.out.println("ERROR hashing URL: " + url);
                }
            }
            
            // Batch fetch all PageRanks in one pass
            Map<String, Double> pageRanks = new HashMap<>();
            for (Map.Entry<String, String> entry : urlHashCache.entrySet()) {
                String url = entry.getKey();
                String hashedURL = entry.getValue();
                try {
                    Row currRow = kvs.getRow("pt-pageranks", hashedURL);
                    double pageRank = 0.0;
                    if (currRow != null && currRow.get("rank") != null) {
                        pageRank = Double.parseDouble(currRow.get("rank"));
                    }
                    pageRanks.put(url, pageRank);
                } catch (Exception e) {
                    System.out.println("ERROR retrieving pageRank for url: " + url);
                    pageRanks.put(url, 0.0);
                }
            }
            
            // Batch fetch all document metadata (titles) in one pass
            Map<String, String> titles = new HashMap<>();
            for (Map.Entry<String, String> entry : urlHashCache.entrySet()) {
                String url = entry.getKey();
                String hashedURL = entry.getValue();
                try {
                    Row metaRow = kvs.getRow("pt-documentStats", hashedURL);
                    String title = "";
                    if (metaRow != null) {
                        String t = metaRow.get("title");
                        if (t != null) title = t;
                    }
                    titles.put(url, title);
                } catch (Exception e) {
                    System.out.println("ERROR retrieving title for url: " + url);
                    titles.put(url, "");
                }
            }
            
            Map<String, Integer> urlToMatchedTermsCount = new HashMap<>();
            if (searchTerms.size() > 1) {
                // For each original search term, get the set of URLs that contain it
                Map<String, Set<String>> termToUrls = new HashMap<>();
                for (String originalTerm : searchTerms) {
                    Set<String> urlsForTerm = new HashSet<>();
                    // Check both original and stemmed versions
                    String lowerTerm = originalTerm.toLowerCase();
                    String stemmedTerm = stemmer.stem(lowerTerm);
                    
                    // Check original term
                    Row row = kvs.getRow("pt-index", lowerTerm);
                    if (row != null) {
                        String rawValue = row.get("_value");
                        if (rawValue == null || rawValue.isEmpty()) {
                            for (String col : row.columns()) {
                                if (col != null && !col.startsWith("_")) {
                                    String val = row.get(col);
                                    if (val != null && !val.isEmpty()) {
                                        rawValue = val;
                                        break;
                                    }
                                }
                            }
                        }
                        if (rawValue != null && !rawValue.isEmpty()) {
                            int firstTab = rawValue.indexOf('\t');
                            if (firstTab >= 0) {
                                int secondTab = rawValue.indexOf('\t', firstTab + 1);
                                if (secondTab >= 0) {
                                    String docList = rawValue.substring(secondTab + 1);
                                    extractUrlsFromDocList(docList, urlsForTerm);
                                }
                            }
                        }
                    }
                    
                    // Check stemmed version if different
                    if (stemmedTerm != null && !stemmedTerm.isEmpty() && !stemmedTerm.equals(lowerTerm)) {
                        Row stemRow = kvs.getRow("pt-index", stemmedTerm);
                        if (stemRow != null) {
                            String rawValue = stemRow.get("_value");
                            if (rawValue == null || rawValue.isEmpty()) {
                                for (String col : stemRow.columns()) {
                                    if (col != null && !col.startsWith("_")) {
                                        String val = stemRow.get(col);
                                        if (val != null && !val.isEmpty()) {
                                            rawValue = val;
                                            break;
                                        }
                                    }
                                }
                            }
                            if (rawValue != null && !rawValue.isEmpty()) {
                                int firstTab = rawValue.indexOf('\t');
                                if (firstTab >= 0) {
                                    int secondTab = rawValue.indexOf('\t', firstTab + 1);
                                    if (secondTab >= 0) {
                                        String docList = rawValue.substring(secondTab + 1);
                                        extractUrlsFromDocList(docList, urlsForTerm);
                                    }
                                }
                            }
                        }
                    }
                    
                    termToUrls.put(originalTerm, urlsForTerm);
                }
                
                for (String url : tfidfSums.keySet()) {
                    int matchedCount = 0;
                    for (Set<String> urls : termToUrls.values()) {
                        if (urls.contains(url)) {
                            matchedCount++;
                        }
                    }
                    urlToMatchedTermsCount.put(url, matchedCount);
                }
            }
            
            // Combine all scores and build results
            for (Map.Entry<String, Double> urlEntry : tfidfSums.entrySet()) {
                String url = urlEntry.getKey();
                double tfidfScore = urlEntry.getValue();
                
                double pageRank = pageRanks.getOrDefault(url, 0.0);
                String title = titles.getOrDefault(url, "");
                String preview = extractPreview(kvs, url, query); // get the preview text of the cached page
                
                double finalScore = tfidfScore * (1.0 + pageRank);
                
                // For multi-term queries, boost results based on how many terms they match
                // All terms = fixed 4.0x boost regardless of query length
                // Partial matches get much lower boost for bigger difference
                if (searchTerms.size() > 1) {
                    int matchedCount = urlToMatchedTermsCount.getOrDefault(url, 0);
                    if (matchedCount == searchTerms.size()) {
                        // if all terms matched then fixed 4.0x boost
                        finalScore *= 4.0;
                    } else if (matchedCount > 0) {
                        // Partial match has much lower boost to create bigger gap
                        // Formula: 0.3 + 0.7 * matchRatio (e.g. 1/4 = 0.475x, 2/4 = 0.65x, 3/4 = 0.825x)
                        double matchRatio = (double) matchedCount / searchTerms.size();
                        finalScore *= (0.3 + 0.7 * matchRatio); // Range: 0.3x (1 term) to 1.0x (all terms, but we handle that above)
                    } else {
                        // No terms matched, give a heavy penalty
                        finalScore *= 0.1;
                    }
                }
                
                // Boost results where query terms appear in the title
                // boost more if more terms appear, highest boost if terms appear in order
                double titleBoost = getTitleBoost(title, searchTerms, stemmer);
                if (titleBoost > 1.0) {
                    finalScore *= titleBoost;
                }
                
                results.add(new SearchResult(url, finalScore, tfidfScore, pageRank, title, preview));
            }


            results.sort((a, b) -> Double.compare(b.totalScore, a.totalScore));          // sort results by descending final score 

            // Cache the results (store full list, not limited)
            synchronized (searchResultCache) {
                // remove oldest entries if cache is too large
                if (searchResultCache.size() >= MAX_CACHE_SIZE) {
                    String firstKey = searchResultCache.keySet().iterator().next();
                    searchResultCache.remove(firstKey);
                }
                // Store full results list in cache
                searchResultCache.put(cacheKey, new ArrayList<>(results));
                System.out.println("[CACHE STORE] Query: \"" + query + "\" - Cached " + results.size() + " results (cache size: " + searchResultCache.size() + ")");
            }

            // Return paginated results (first page by default for better latency)
            int totalResults = results.size();
            int startIndex = (page - 1) * pageSize;
            int endIndex = Math.min(startIndex + pageSize, totalResults);
            
            System.out.println("[CACHE MISS] Query: \"" + query + "\" - Page " + page + ", pageSize " + pageSize + ", startIndex " + startIndex + ", endIndex " + endIndex + ", totalResults " + totalResults);
            
            List<SearchResult> pageResults;
            if (startIndex >= totalResults) {
                // Page beyond available results
                pageResults = new ArrayList<>();
            } else {
                pageResults = new ArrayList<>(results.subList(startIndex, endIndex));
            }
            
            System.out.println("[CACHE MISS] Query: \"" + query + "\" - Returning page " + page + " (" + pageResults.size() + " of " + totalResults + " results, indices " + startIndex + "-" + (endIndex - 1) + ")");
            return pageResults;

    }



    private static List<String> tokenizeUserSearch(String query){
        String cleaned = query.replaceAll("[^A-Za-z0-9\\s]", " ").toLowerCase();
        
        List<String> toks = new ArrayList<>();
        int start = 0;
        while (start < cleaned.length()) {
            // Skip whitespace
            while (start < cleaned.length() && cleaned.charAt(start) == ' ') start++;
            if (start >= cleaned.length()) break;
            
            // Find end of token
            int end = start;
            while (end < cleaned.length() && cleaned.charAt(end) != ' ') end++;
            
            String token = cleaned.substring(start, end);
            if (!token.isEmpty()) {
                toks.add(token);
            }
            start = end + 1;
        }
        return toks;
    }
    
    /**
     * Calculate boost multiplier based on how many query terms appear in the title
     * and whether they appear in order. Higher boost for more terms, highest for ordered matches.
     */
    private static double getTitleBoost(String title, List<String> searchTerms, PorterStemmer stemmer) {
        if (title == null || title.isEmpty() || searchTerms.isEmpty()) {
            return 1.0;
        }
        
        // Tokenize the title
        List<String> titleTokens = tokenizeUserSearch(title);
        if (titleTokens.isEmpty()) {
            return 1.0;
        }
        
        // Create maps: term -> list of positions in title where it appears
        Map<String, List<Integer>> termPositions = new HashMap<>();
        
        // For each search term, find all positions in title where it (or its stem) appears
        for (int i = 0; i < searchTerms.size(); i++) {
            String term = searchTerms.get(i).toLowerCase();
            List<Integer> positions = new ArrayList<>();
            
            // Check original term
            for (int j = 0; j < titleTokens.size(); j++) {
                String titleToken = titleTokens.get(j);
                if (titleToken.equals(term)) {
                    positions.add(j);
                }
            }
            
            // Check stemmed versions
            String stemmedTerm = stemmer.stem(term);
            if (stemmedTerm != null && !stemmedTerm.isEmpty()) {
                for (int j = 0; j < titleTokens.size(); j++) {
                    String titleToken = titleTokens.get(j);
                    String stemmedTitleToken = stemmer.stem(titleToken);
                    if (stemmedTitleToken != null && stemmedTitleToken.equals(stemmedTerm)) {
                        if (!positions.contains(j)) {
                            positions.add(j);
                        }
                    }
                }
            }
            
            if (!positions.isEmpty()) {
                termPositions.put(term, positions);
            }
        }
        
        if (termPositions.isEmpty()) {
            return 1.0; // No terms found in title
        }
        
        int matchingTerms = termPositions.size();
        
        //give much stronger boost when term is in title
        if (searchTerms.size() == 1 && matchingTerms == 1) {
            String term = searchTerms.get(0).toLowerCase();
            List<Integer> positions = termPositions.get(term);
            if (positions != null && !positions.isEmpty()) {
                // check if term appears as the first word in title
                boolean isFirstWord = positions.contains(0);
                if (isFirstWord) {
                    return 3.5; // very strong boost when term is the first word in title
                }
                // check if term appears early in title (first 3 words) - strong boost
                boolean earlyInTitle = positions.stream().anyMatch(pos -> pos < 3);
                if (earlyInTitle) {
                    return 2.5; // strong boost for single-word queries where term is early in title
                } else {
                    return 2.0; // Still strong boost if term is in title but not early
                }
            }
        }
        
        // For multi-word queries give strong boost when all terms appear in title
        if (searchTerms.size() > 1 && matchingTerms == searchTerms.size()) {
            // Check if terms appear in order
            boolean inOrder = checkTermsInOrder(searchTerms, termPositions);
            
            // Check if all terms appear early in title (first few words)
            boolean allEarly = true;
            for (String term : searchTerms) {
                List<Integer> positions = termPositions.get(term.toLowerCase());
                if (positions == null || positions.isEmpty()) {
                    allEarly = false;
                    break;
                }
                // Check if term appears in first 4 words of title
                boolean termEarly = positions.stream().anyMatch(pos -> pos < 4);
                if (!termEarly) {
                    allEarly = false;
                    break;
                }
            }
            
            if (inOrder && allEarly) {
                return 4.0; // Very strong boost for exact title matches
            } else if (inOrder) {
                // All terms in title and in order
                return 3.0;
            } else if (allEarly) {
                // All terms early but not in order
                return 2.5;
            } else {
                // All terms in title but not early/ordered
                return 2.0;
            }
        }
        
        double baseBoost = 1.0 + (0.2 * matchingTerms); // 20% per matching term
        
        // Check if terms appear in order (for partial matches)
        boolean inOrder = checkTermsInOrder(searchTerms, termPositions);
        if (inOrder && matchingTerms == searchTerms.size()) {
            // All terms found and in order - highest boost
            return baseBoost * 1.85; // Additional 85% multiplier for ordered match
        } else if (inOrder) {
            // Some terms in order - moderate additional boost
            return baseBoost * 1.30; // Additional 30% multiplier
        }
        
        return baseBoost;
    }
    
    /**
     * Extract URLs from a docList string (format: url1|...,url2|...)
     */
    private static void extractUrlsFromDocList(String docList, Set<String> urlSet) {
        if (docList == null || docList.isEmpty()) return;
        
        int docStart = 0;
        while (docStart < docList.length()) {
            int docEnd = docList.indexOf(',', docStart);
            if (docEnd < 0) docEnd = docList.length();
            
            String doc = docList.substring(docStart, docEnd).trim();
            docStart = docEnd + 1;
            
            if (doc.isEmpty()) continue;
            
            int urlSplit = doc.indexOf("|");
            if (urlSplit < 0) continue;
            
            String url = doc.substring(0, urlSplit);
            if (!url.isEmpty()) {
                urlSet.add(url);
            }
        }
    }
    
    /**
     * Check if the search terms appear in the title in the same order as the query
     */
    private static boolean checkTermsInOrder(List<String> searchTerms, Map<String, List<Integer>> termPositions) {
        if (searchTerms.size() < 2) {
            return false; // Need at least 2 terms to check order
        }
        
        // Find the earliest position for each term
        List<Integer> earliestPositions = new ArrayList<>();
        for (String term : searchTerms) {
            List<Integer> positions = termPositions.get(term.toLowerCase());
            if (positions == null || positions.isEmpty()) {
                return false; // Term not found
            }
            earliestPositions.add(Collections.min(positions));
        }
        
        // Check if positions are in ascending order (allowing some flexibility)
        for (int i = 0; i < earliestPositions.size() - 1; i++) {
            if (earliestPositions.get(i) >= earliestPositions.get(i + 1)) {
                return false; // Not in order
            }
        }
        
        return true; // All terms found and in order
    }


    /**
     * loads stop words from the stopwords.txt file
     * @return returns a set of stop words
     * @author Evan Law
     */
    private static Set<String> loadStopWords() {
        Set<String> stopWords = new HashSet<>();
        String resourcePath = "cis5550/resources/stopwords.txt";
        try (InputStream in = SearchEngine.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (in == null) {
                System.err.println("[Aaron’s Indexer] Stopword file not found at " + resourcePath);
                return stopWords;
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) { // read each line of the file
                    String trimmed = line.trim().toLowerCase(Locale.US);
                    if (!trimmed.isEmpty() && !trimmed.startsWith("#")) { // skip empty lines and starting with # aka comments
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
    
    private static boolean looksReasonable(String s) {
        // reject anything containing digits or >20 chars
        if (s.length() > 20) return false;
        if (!s.matches("^[a-z]+$")) return false;

        return true;
    }
    
    public static synchronized void loadAllTerms(KVSClient kvs) throws Exception {
        if (termsLoaded) return;

        System.out.println("[SearchEngine] Loading autocomplete terms...");
        long startTime = System.currentTimeMillis();
        
        PorterStemmer stemmer = new PorterStemmer();
        Iterator<Row> iter = kvs.scan("pt-index");
        while (iter.hasNext()) {
            Row r = iter.next();
            String term = r.key();
            if (term != null && looksReasonable(term)) {
                allTerms.add(term);
                
                // build stem to word mapping for finding related word forms
                String termLower = term.toLowerCase();
                String stem = stemmer.stem(termLower);
                if (stem != null && !stem.isEmpty() && !stem.equals(termLower)) {
                    synchronized (stemToWords) {
                        stemToWords.computeIfAbsent(stem, k -> new HashSet<>()).add(termLower);
                    }
                }
            }
        }

        Collections.sort(allTerms); // optional but helps binary search
        termsLoaded = true;
        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("[SearchEngine] Loaded " + allTerms.size() + " autocomplete terms in " + loadTime + " ms");
        System.out.println("[SearchEngine] Built stem-to-words mapping with " + stemToWords.size() + " unique stems");
    }
    
    public static List<String> getSuggestions(KVSClient kvs, String prefix, int limit) throws Exception {
        if (!termsLoaded) loadAllTerms(kvs);

        prefix = prefix.toLowerCase().trim();
        if (prefix.isEmpty()) {
            return new ArrayList<>();
        }
        
        // Split the prefix into words
        List<String> words = tokenizeUserSearch(prefix);
        
        List<String> out = new ArrayList<>();
        
        if (words.isEmpty()) {
            return out;
        }
        
        // If single word, use the original logic
        if (words.size() == 1) {
            String singlePrefix = words.get(0);
            
            // binary search starting point
            int idx = Collections.binarySearch(allTerms, singlePrefix);
            if (idx < 0) idx = -(idx + 1);

            // walk forward until prefix no longer matches
            for (int i = idx; i < allTerms.size() && out.size() < limit; i++) {
                String t = allTerms.get(i);
                if (!t.startsWith(singlePrefix)) break;
                out.add(t);
            }
        } else {
            // Multiple words: all but the last are complete, suggest completions for the last word
            String lastWord = words.get(words.size() - 1);
            String prefixWords = String.join(" ", words.subList(0, words.size() - 1));
            
            // Find suggestions for the last word
            int idx = Collections.binarySearch(allTerms, lastWord);
            if (idx < 0) idx = -(idx + 1);

            // Build multi-word suggestions
            for (int i = idx; i < allTerms.size() && out.size() < limit; i++) {
                String t = allTerms.get(i);
                if (!t.startsWith(lastWord)) break;
                // Combine previous words with the suggested completion
                out.add(prefixWords + " " + t);
            }
        }

        return out;
    }
    
    // Spellcheck EC
    public static String spellcheck(KVSClient kvs, String word) throws Exception {
        if (!termsLoaded) loadAllTerms(kvs);

        word = word.toLowerCase();
        
        // Do not spellcheck if word already exists in our dictionary
        if (allTerms.contains(word)) {
            return null;
        }
        
        int bestDist = 3;          // we accept distance 1 or 2 only
        String bestMatch = null;

        for (String term : allTerms) {
            int d = levenshtein(word, term);
            if (d < bestDist) {
                bestDist = d;
                bestMatch = term;
            }
            if (d == 1) break; // cannot get better than edit distance 1
        }

        return (bestDist <= 2) ? bestMatch : null;
    }
    
    public static String spellcheckQuery(KVSClient kvs, String query) throws Exception {
        List<String> words = tokenizeUserSearch(query);
        boolean changed = false;

        List<String> corrected = new ArrayList<>();
        for (String w : words) {
            String suggestion = spellcheck(kvs, w);
            if (suggestion != null && !suggestion.equalsIgnoreCase(w)) {
                corrected.add(suggestion);
                changed = true;
            } else {
                corrected.add(w);
            }
        }

        return changed ? String.join(" ", corrected) : null;
    }

    // Standard Levenshtein distance
    private static int levenshtein(String a, String b) {
        int[][] dp = new int[a.length()+1][b.length()+1];

        for (int i = 0; i <= a.length(); i++) dp[i][0] = i;
        for (int j = 0; j <= b.length(); j++) dp[0][j] = j;

        for (int i = 1; i <= a.length(); i++) {
            for (int j = 1; j <= b.length(); j++) {

                int cost = (a.charAt(i-1) == b.charAt(j-1)) ? 0 : 1;

                dp[i][j] = Math.min(
                    Math.min(
                        dp[i-1][j] + 1,    // deletion
                        dp[i][j-1] + 1     // insertion
                    ),
                    dp[i-1][j-1] + cost    // substitution
                );
            }
        }
        return dp[a.length()][b.length()];
    }

    /**
     * Extract preview text from cached page content
     * So if the query is "computer programming", and the text is "*lots of text* computer programming is the best way to learn programming", the snippet will have that section
     */
    public static String extractPreview(KVSClient kvs, String url, String query) {
        try {
            String hashed = Hasher.hash(url).split("\n")[0];
            Row crawlRow = kvs.getRow("pt-crawl", hashed);
            if (crawlRow == null) return "";
            
            String htmlContent = crawlRow.get("page");
            if (htmlContent == null || htmlContent.isEmpty()) return "";
    

            String cleaned = SCRIPT_STYLE.matcher(htmlContent).replaceAll(" ");
            cleaned = COMMENTS.matcher(cleaned).replaceAll(" ");
            cleaned = TAGS.matcher(cleaned).replaceAll(" ");
            String text = WHITESPACE.matcher(cleaned).replaceAll(" ").trim();
            
            if (text.length() < 50) return "";
            
            // find query terms in text
            String[] terms = query.toLowerCase().split("\\s+");
            String lowerText = text.toLowerCase();
            int bestPos = -1;
            
            // find the best position of the query terms in the text
            for (String term : terms) {
                if (term.length() >= 2) {
                    int pos = lowerText.indexOf(term);
                    if (pos >= 0) {
                        bestPos = pos;
                        break;
                    }
                }
            }
            
            // get snippet around query term or from start
            // if query is not found return from the start of the text
            int start = bestPos >= 0 ? Math.max(0, bestPos - 80) : 0;
            int end = Math.min(text.length(), start + 350);
            String snippet = text.substring(start, end);
            
            if (start > 0) snippet = "..." + snippet;
            if (end < text.length()) snippet = snippet + "...";
            
            return snippet;
        } catch (Exception e) {
            return "";
        }
    }

}