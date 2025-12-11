package cis5550.frontend;

import static cis5550.webserver.Server.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import cis5550.frontend.SearchEngine.SearchResult;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.webserver.Server.staticFiles;

public class SauronFrontend {

    private static KVSClient kvs;    

    public static void main(String args[]) throws Exception {


        kvs = new KVSClient(args[0]);


        // Decide ports based on environment
        String env = System.getProperty("sauron.env", "dev");

        if ("prod".equalsIgnoreCase(env)) {
            port(80);          // initialize HTTP port 
            securePort(443);   // initialize HTTPS port (needs keystore.jks in working dir)
            System.out.println("[SauronFrontend] Starting in PROD mode on ports 80 (HTTP) and 443 (HTTPS)");
        } else {
            port(8080);
            System.out.println("[SauronFrontend] Starting in DEV mode on port 8080");
        }

        staticFiles.location("staticFiles");
        

            



        //-------------------------------------------------------------------------------------------------------------------------
        //------------------------------ H O M E    P A G E -----------------------------------------------------------------------
        get("/", (req, res) -> {
            return Files.readString(Path.of("staticFiles/index.html"));   // return the index.html file from staticFiles folder
        });
        
	     // -----------------------------------------------------------
	     // AUTOCOMPLETE SEARCH SUGGESTIONS
	     // -----------------------------------------------------------
	     get("/suggest", (req, res) -> {
	         String prefix = req.queryParams("q");
	
	         if (prefix == null || prefix.trim().isEmpty()) {
	             res.status(400, "Bad Request");
	             return "[]";
	         }
	         
	         if (prefix.length() < 3) {
	        	    res.header("Content-Type", "application/json");
	        	    return "[]";  // don't scan KVS for short prefixes
	         }
	
	         prefix = prefix.toLowerCase();
	         List<String> suggestions = SearchEngine.getSuggestions(kvs, prefix, 10);
	
	         res.header("Content-Type", "application/json");
	         return "[" + String.join(",", suggestions.stream()
	                 .map(s -> "\"" + escape(s) + "\"")
	                 .toList()) + "]";
	     });


        //-------------------------------------------------------------------------------------------------------------------------
        //------------------------------ S E A R C H    R E S U L T S -------------------------------------------------------------
        get("/search", (req, res) -> {

            String userQuery = req.queryParams("userQuery");

            if (userQuery == null || userQuery.trim().isEmpty()) {
                res.status(400, "Bad Request");
                return "<html><body><p>Empty query.</p><p><a href=\"/\">Back</a></p></body></html>";
            }

            // Get pagination parameters (15 results per page)
            String pageParam = req.queryParams("page");
            String pageSizeParam = req.queryParams("pageSize");
            int page = 1;
            int pageSize = 15;
            try {
                if (pageParam != null && !pageParam.trim().isEmpty()) {
                    page = Integer.parseInt(pageParam.trim());
                    if (page < 1) page = 1;
                }
                if (pageSizeParam != null && !pageSizeParam.trim().isEmpty()) {
                    pageSize = Integer.parseInt(pageSizeParam.trim());
                    // Cap page size to prevent abuse
                    if (pageSize > 200) pageSize = 200;
                    if (pageSize < 1) pageSize = 15;
                }
            } catch (NumberFormatException e) {
                // Use defaults if parsing fails
                System.err.println("[SauronFrontend] Error parsing pagination params - page: " + pageParam + ", pageSize: " + pageSizeParam + ", using defaults");
            }
            
            System.out.println("[SauronFrontend] Search request - Query: \"" + userQuery + "\", Page: " + page + ", PageSize: " + pageSize);
            List<SearchResult> results = SearchEngine.executeSearch(kvs, userQuery, 200, page, pageSize);

            // Read the SVG content from index.html
            String svgContent = "";
            try {
                String indexHtml = Files.readString(Path.of("staticFiles/index.html"));
                int svgStart = indexHtml.indexOf("<svg");
                int svgEnd = indexHtml.indexOf("</svg>", svgStart) + 6;
                if (svgStart > 0 && svgEnd > svgStart) {
                    svgContent = indexHtml.substring(svgStart, svgEnd);
                }
            } catch (Exception e) {
                System.err.println("Failed to read SVG: " + e.getMessage());
            }

            StringBuilder htmlSB = new StringBuilder();
            htmlSB.append("<!doctype html><html><head>")
                  .append("<meta charset=\"UTF-8\">")
                  .append("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">")
                  .append("<title>AskSauron - Results</title>")
                  .append("<style>")
                  .append("body { margin: 0; padding: 20px; background: #1a1a1a; color: #e0e0e0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; line-height: 1.6; overflow-x: hidden; box-sizing: border-box; }")
                  .append("* { box-sizing: border-box; }")
                  .append(".results-container { display: flex; gap: 30px; max-width: 1400px; margin: 0 auto; width: 100%; box-sizing: border-box; padding: 0 20px; }")
                  .append(".results-column { flex: 1; min-width: 0; overflow-wrap: break-word; word-wrap: break-word; }")
                  .append(".tower-column { flex: 0 0 300px; position: sticky; top: 20px; height: fit-content; align-self: flex-start; }")
                  .append(".tower-column svg { width: 100%; height: auto; max-width: 300px; }")
                  .append("h2 { color: #ff6b00; margin-top: 0; font-size: 1.8em; font-weight: 600; overflow-wrap: break-word; word-wrap: break-word; }")
                  .append("ol { padding-left: 25px; margin: 20px 0; }")
                  .append("li { margin-bottom: 20px; padding: 15px; background: rgba(255, 255, 255, 0.03); border-radius: 8px; border-left: 3px solid rgba(255, 107, 0, 0.3); overflow-wrap: break-word; word-wrap: break-word; max-width: 100%; box-sizing: border-box; }")
                  .append(".url-primary { color: #60a5fa !important; font-size: 1.1em; font-weight: 500; text-decoration: none; overflow-wrap: break-word; word-wrap: break-word; word-break: break-word; display: inline-block; max-width: 100%; }")
                  .append(".url-primary:hover { color: #93c5fd !important; text-decoration: underline; }")
                  .append(".url-primary strong { color: #fbbf24 !important; font-weight: 600; overflow-wrap: break-word; word-wrap: break-word; }")
                  .append(".url-primary:hover strong { color: #fcd34d !important; }")
                  .append(".url-secondary { color: #34d399 !important; font-size: 0.9em; overflow-wrap: break-word; word-wrap: break-word; word-break: break-all; display: inline-block; max-width: 100%; }")
                  .append(".score-info { font-size: 0.85em; color: #9ca3af !important; margin-top: 5px; overflow-wrap: break-word; word-wrap: break-word; }")
                  .append(".preview-text { color: #d1d5db !important; font-size: 0.95em; line-height: 1.5; margin: 8px 0; overflow-wrap: break-word; word-wrap: break-word; }")
                  .append(".preview-text .highlight { background-color: rgba(255, 107, 0, 0.3); color: #fbbf24; font-weight: 500; padding: 0 2px; border-radius: 2px; }")
                  .append(".result-links { font-size: 0.85em; margin-top: 8px; color: #9ca3af; }")
                  .append(".result-links a { color: #9ca3af; text-decoration: none; margin-right: 12px; }")
                  .append(".result-links a:hover { color: #60a5fa; text-decoration: underline; }")
                  .append("p { color: #d1d5db; }")
                  .append("a { color: #60a5fa; text-decoration: none; }")
                  .append("a:hover { color: #93c5fd; text-decoration: underline; }")
                  .append("@media (max-width: 768px) { .results-container { flex-direction: column; } .tower-column { position: relative; } }")
                  .append("</style>")
                  .append("</head><body>");

            htmlSB.append("<div class=\"results-container\">");
            htmlSB.append("<div class=\"results-column\">");
            htmlSB.append("<h2>The One Eye shows: ").append(escape(userQuery)).append("</h2>");
            htmlSB.append("<p><a href=\"/\">← Back to Sauron's Gaze</a></p>");

            if (results.isEmpty()) {
                // Spellcheck try to suggest the closest dictionary word
                String suggestion = SearchEngine.spellcheckQuery(kvs, userQuery);

                if (suggestion != null && !suggestion.equalsIgnoreCase(userQuery)) {
                    htmlSB.append("<p>The Eye searches… yet the void answers.</p>");
                    htmlSB.append("<p>The One Eye sees clearer...Did you mean: <a href=\"/search?userQuery=")
                          .append(escape(suggestion))
                          .append("\"><strong>")
                          .append(escape(suggestion))
                          .append("</strong></a> ?</p>");
                } else {
                    htmlSB.append("<p>The Eye searches… yet the void answers.</p>");
                }
            } else {
                // Get total results count for display
                int totalResults = SearchEngine.getCachedResultCount(userQuery);
                
                // Add display text at the top showing current range
                if (totalResults > 0) {
                    int startRange = (page - 1) * pageSize + 1;
                    int endRange = Math.min(page * pageSize, totalResults);
                    htmlSB.append("<p style=\"color: #9ca3af; margin-bottom: 15px; font-size: 0.95em;\">");
                    htmlSB.append("Showing ").append(startRange).append("-").append(endRange);
                    htmlSB.append(" of ").append(totalResults).append(" results");
                    htmlSB.append("</p>");
                }
                
                // Set the starting number for the ordered list based on the current page
                int startNumber = (page - 1) * pageSize + 1;
                htmlSB.append("<ol start=\"").append(startNumber).append("\">");
                for (SearchResult result : results) {

                    String url = result.url;
                    String title = (result.title != null && !result.title.isEmpty())
                            ? result.title
                            : url;  // fallback if a title didn't get extracted
                    String preview = (result.preview != null) ? result.preview : "";

                    htmlSB.append("<li>");

                    // Title as the main clickable element
                    htmlSB.append("<a href=\"")
                          .append(escape(url))
                          .append("\" class=\"url-primary\"><strong>")
                          .append(escape(title))
                          .append("</strong></a><br>");

                    // Show URL below title 
                    htmlSB.append("<span class=\"url-secondary\">")
                          .append(escape(url))
                          .append("</span><br>");

                    // Preview text
                    if (!preview.isEmpty()) {
                        htmlSB.append("<div class=\"preview-text\">")
                              .append(highlightSearchTerms(escape(preview), userQuery))
                              .append("</div>");
                    }

                    // Cached link
                    String hashed = hashURL(url);
                    if (hashed != null && kvs.getRow("pt-crawl", hashed) != null) {
                        htmlSB.append("<div class=\"result-links\">")
                              .append("<a href=\"/cached?url=").append(java.net.URLEncoder.encode(url, "UTF-8"))
                              .append("&query=").append(java.net.URLEncoder.encode(userQuery, "UTF-8")).append("\"> Cached Page</a>")
                              .append("</div>");
                    }

                    // // Debug scoring info
                    // htmlSB.append("<span class=\"score-info\">")
                    //       .append("score=")
                    //       .append(String.format("%.5f", result.totalScore))
                    //       .append(" (tfidf=")
                    //       .append(String.format("%.5f", result.tfidfScore))
                    //       .append(", pagerank=")
                    //       .append(String.format("%.5f", result.pageRankScore))
                    //       .append(")")
                    //       .append("</span>");

                    htmlSB.append("</li>");
                }
                htmlSB.append("</ol>");
                
                // Add pagination controls
                if (totalResults > 0) {
                    int totalPages = (int) Math.ceil((double) totalResults / pageSize);
                    htmlSB.append("<div style=\"margin-top: 30px; padding: 20px; background: rgba(255, 255, 255, 0.03); border-radius: 8px; text-align: center;\">");
                    htmlSB.append("<p style=\"color: #9ca3af; margin-bottom: 15px;\">");
                    htmlSB.append("Showing ").append((page - 1) * pageSize + 1).append("-").append(Math.min(page * pageSize, totalResults));
                    htmlSB.append(" of ").append(totalResults).append(" results");
                    htmlSB.append("</p>");
                    
                    htmlSB.append("<div style=\"display: flex; justify-content: center; gap: 10px; flex-wrap: wrap;\">");
                    
                    // Previous button
                    if (page > 1) {
                        htmlSB.append("<a href=\"/search?userQuery=").append(java.net.URLEncoder.encode(userQuery, "UTF-8"))
                              .append("&page=").append(page - 1).append("&pageSize=").append(pageSize)
                              .append("\" style=\"padding: 8px 16px; background: #a63838; color: white; text-decoration: none; border-radius: 4px;\">← Previous</a>");
                    }
                    
                    // Page numbers (show current page and a few around it)
                    int startPage = Math.max(1, page - 2);
                    int endPage = Math.min(totalPages, page + 2);
                    
                    if (startPage > 1) {
                        htmlSB.append("<a href=\"/search?userQuery=").append(java.net.URLEncoder.encode(userQuery, "UTF-8"))
                              .append("&page=1&pageSize=").append(pageSize)
                              .append("\" style=\"padding: 8px 12px; background: rgba(255, 255, 255, 0.1); color: #60a5fa; text-decoration: none; border-radius: 4px;\">1</a>");
                        if (startPage > 2) {
                            htmlSB.append("<span style=\"color: #9ca3af; padding: 8px;\">...</span>");
                        }
                    }
                    
                    for (int p = startPage; p <= endPage; p++) {
                        if (p == page) {
                            htmlSB.append("<span style=\"padding: 8px 12px; background: #ff6b00; color: white; border-radius: 4px;\">").append(p).append("</span>");
                        } else {
                            htmlSB.append("<a href=\"/search?userQuery=").append(java.net.URLEncoder.encode(userQuery, "UTF-8"))
                                  .append("&page=").append(p).append("&pageSize=").append(pageSize)
                                  .append("\" style=\"padding: 8px 12px; background: rgba(255, 255, 255, 0.1); color: #60a5fa; text-decoration: none; border-radius: 4px;\">").append(p).append("</a>");
                        }
                    }
                    
                    if (endPage < totalPages) {
                        if (endPage < totalPages - 1) {
                            htmlSB.append("<span style=\"color: #9ca3af; padding: 8px;\">...</span>");
                        }
                        htmlSB.append("<a href=\"/search?userQuery=").append(java.net.URLEncoder.encode(userQuery, "UTF-8"))
                              .append("&page=").append(totalPages).append("&pageSize=").append(pageSize)
                              .append("\" style=\"padding: 8px 12px; background: rgba(255, 255, 255, 0.1); color: #60a5fa; text-decoration: none; border-radius: 4px;\">").append(totalPages).append("</a>");
                    }
                    
                    // Next button
                    if (page < totalPages) {
                        htmlSB.append("<a href=\"/search?userQuery=").append(java.net.URLEncoder.encode(userQuery, "UTF-8"))
                              .append("&page=").append(page + 1).append("&pageSize=").append(pageSize)
                              .append("\" style=\"padding: 8px 16px; background: #a63838; color: white; text-decoration: none; border-radius: 4px;\">Next →</a>");
                    }
                    
                    htmlSB.append("</div>");
                    htmlSB.append("</div>");
                }
            }
            
            htmlSB.append("</div>"); // Close results-column
            
            // Add the tower SVG on the right
            htmlSB.append("<div class=\"tower-column\">");
            if (!svgContent.isEmpty()) {
                htmlSB.append(svgContent);
            }
            htmlSB.append("</div>"); // Close tower-column
            
            htmlSB.append("</div>"); // Close results-container
            htmlSB.append("</body></html>");
            return htmlSB.toString();
        });
        
        // Preload autocomplete terms on startup to avoid first-request delay
        System.out.println("[SauronFrontend] Preloading autocomplete terms...");
        try {
            SearchEngine.loadAllTerms(kvs);
            System.out.println("[SauronFrontend] Autocomplete preload complete");
        } catch (Exception e) {
            System.err.println("[SauronFrontend] Failed to preload autocomplete: " + e.getMessage());
        } 

        //-------------------------------------------------------------------------------------------------------------------------
        //------------------------------ C A C H E D    P A G E    V I E W E R  -------------------------------------------------------------
        get("/cached", (req, res) -> {
            String urlParam = req.queryParams("url");
            if (urlParam == null || urlParam.trim().isEmpty()) {
                res.status(400, "Bad Request");
                return "<html><body style=\"background: #1a1a1a; color: #e0e0e0; padding: 20px;\"><p>No URL provided.</p><p><a href=\"/\" style=\"color: #60a5fa;\">Back</a></p></body></html>";
            }
            // get the query parameter from the request
            String queryParam = req.queryParams("query");
            String backLink = "/";
            if (queryParam != null && !queryParam.trim().isEmpty()) {
                backLink = "/search?userQuery=" + java.net.URLEncoder.encode(queryParam, "UTF-8");
            }
            // decode the url parameter from the request and hash
            String url = java.net.URLDecoder.decode(urlParam, "UTF-8");
            String hashed = hashURL(url);
            if (hashed == null) {
                res.status(500, "Internal Server Error");
                return "<html><body style=\"background: #1a1a1a; color: #e0e0e0; padding: 20px;\"><p>Error hashing URL.</p><p><a href=\"" + backLink + "\" style=\"color: #60a5fa;\">Back</a></p></body></html>";
            }

            // get the row from the kvs
            try {
                Row crawlRow = kvs.getRow("pt-crawl", hashed);
                if (crawlRow == null) {
                    res.status(404, "Not Found");
                    return "<html><body style=\"background: #1a1a1a; color: #e0e0e0; padding: 20px;\"><p>Cached page not found.</p><p><a href=\"" + backLink + "\" style=\"color: #60a5fa;\">Back</a></p></body></html>";
                }

                // get the html content of the cached page
                String htmlContent = crawlRow.get("page");
                if (htmlContent == null || htmlContent.isEmpty()) {
                    res.status(404, "Not Found");
                    return "<html><body style=\"background: #1a1a1a; color: #e0e0e0; padding: 20px;\"><p>No cached content available.</p><p><a href=\"" + backLink + "\" style=\"color: #60a5fa;\">Back</a></p></body></html>";
                }

                // Build the HTML content for the cached page
                StringBuilder htmlSB = new StringBuilder();
                htmlSB.append("<!doctype html><html><head>")
                      .append("<meta charset=\"UTF-8\">")
                      .append("<title>Cached: ").append(escape(url)).append("</title>")
                      .append("<style>body { margin: 20px; background: #1a1a1a; color: #e0e0e0; font-family: Arial, sans-serif; }")
                      .append(".header { background: rgba(255, 107, 0, 0.1); padding: 15px; border-radius: 8px; margin-bottom: 20px; }")
                      .append(".header h2 { color: #ff6b00; margin: 0 0 10px 0; }")
                      .append(".header a { color: #60a5fa; }")
                      .append(".content { background: rgba(255, 255, 255, 0.05); padding: 20px; border-radius: 8px; }")
                      .append(".content pre { white-space: pre-wrap; word-wrap: break-word; }")
                      .append("</style></head><body>");

                // build the html content for the cached page
                htmlSB.append("<div class=\"header\">");
                htmlSB.append("<h2>Cached Page View</h2>");
                htmlSB.append("<p><a href=\"").append(escape(url)).append("\" target=\"_blank\">").append(escape(url)).append("</a></p>");
                htmlSB.append("<p><a href=\"").append(backLink).append("\">← Return to what the Eye revealed</a></p>");
                htmlSB.append("</div>");

                htmlSB.append("<div class=\"content\">");
                htmlSB.append("<pre>").append(escape(htmlContent)).append("</pre>");
                htmlSB.append("</div></body></html>");
                return htmlSB.toString();
            } catch (Exception e) {
                res.status(500, "Internal Server Error");
                return "<html><body style=\"background: #1a1a1a; color: #e0e0e0; padding: 20px;\"><p>Error: " + escape(e.getMessage()) + "</p><p><a href=\"/\" style=\"color: #60a5fa;\">Back</a></p></body></html>";
            }
        });

    }

    // hash the url to a string
    private static String hashURL(String url) {
        try {
            return Hasher.hash(url).split("\n")[0];
        } catch (Exception e) {
            return null;
        }
    }

    private static String escape(String s) {
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;");
    }

    private static String highlightSearchTerms(String text, String query) {
        if (text == null || text.isEmpty() || query == null || query.trim().isEmpty()) {
            return text;
        }
        
        String[] terms = query.toLowerCase().trim().split("\\s+");
        String result = text;
        
        for (String term : terms) {
            if (term.length() >= 2) {
                String pattern = "(?i)\\b(" + java.util.regex.Pattern.quote(term) + ")\\b";
                result = result.replaceAll(pattern, "<span class=\"highlight\">$1</span>");
            }
        }
        
        return result;
    }

}
