package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.HTTP;
import cis5550.tools.URLParser;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.*;
import java.util.zip.GZIPInputStream;

public class Crawler {

	private static final Pattern HREF_RE = Pattern.compile("(?i)\\bhref\\s*=\\s*(\"([^\"]*)\"|'([^']*)'|([^\\s>]+))");
	private static final Pattern A_TAG_RE = Pattern.compile("(?is)<a\\b([^>]*)>(.*?)</a>");
	private static final Set<String> SKIP_EXT = Set.of(".jpg", ".jpeg", ".gif", ".png", ".txt");
	private static final int MIN_PAGE_SIZE = 200;
	private static final int MAX_PAGE_SIZE = 10000000;
	// Calendar and archive patterns to skip
	private static final Pattern CALENDAR_PATTERN = Pattern.compile("(?i)/(\\d{4})/(\\d{1,2})(?:/|\\?|$)",
			Pattern.CASE_INSENSITIVE);
	private static final Pattern ARCHIVE_PATTERN = Pattern.compile("(?i)/archive|/calendar|/events|/schedule",
			Pattern.CASE_INSENSITIVE);
	private static final String HOSTS_TABLE = "hosts";
	private static final String LAST_ACCESS = "lastAccess";
	private static final long GAP_MS = 500L;
	private static final String ROBOTS_TEXT = "robotsText";
	private static final String ROBOTS_FETCH = "robotsFetched";
	private static final String CRAWL_DELAY = "crawlDelayMs";
	private static final String CONTENT_TABLE = "pt-content";
	private static final String CANONICAL_COL = "canonicalURL";
	private static final String BLACKLIST_TABLE = "blacklisted-urls";

	// read crawl delay from host row
	private static long readCrawlDelayMs(Row hostRow) {
		try {
			if (hostRow != null) {
				String d = hostRow.get(CRAWL_DELAY);
				if (d != null && !d.isEmpty()) {
					return Long.parseLong(d);
				}
			}
		} catch (Exception e) {

		}
		return GAP_MS;
	}

	// check if crawl delay is needed
	private static boolean needDelay(Row hostRow, long now) {
		try {
			if (hostRow == null) {
				return false;
			}
			String ts = hostRow.get(LAST_ACCESS);
			if (ts == null) {
				return false;
			}
			long last = Long.parseLong(ts);
			long gapMs = readCrawlDelayMs(hostRow);
			return now - last < gapMs;
		} catch (Exception e) {
			return false;
		}
	}

	// parse crawl delay from robots text
	private static Long parseCrawlDelayMS(String robotsText, String ua) {
		if (robotsText == null) {
			return null;
		}

		String[] lines = robotsText.replace("\r", "").split("\n");

		boolean inMatchingBlock = false;
		for (String raw : lines) {
			String line = raw.split("#", 2)[0].trim();
			if (line.isEmpty()) {
				continue;
			}

			int colon = line.indexOf(':');
			if (colon < 0) {
				continue;
			}

			String key = line.substring(0, colon).trim().toLowerCase(Locale.ROOT);
			String val = line.substring(colon + 1).trim();

			switch (key) {
				case "user-agent":
					String uaVal = val.toLowerCase(Locale.ROOT);
					inMatchingBlock = uaVal.equals(ua.toLowerCase(Locale.ROOT)) || uaVal.equals("*");
					break;

				case "crawl-delay":
					if (inMatchingBlock) {
						try {
							double secs = Double.parseDouble(val);
							long ms = (long) Math.round(secs * 1000.0);
							if (ms < 0) {
								ms = GAP_MS;
							}
							return ms;
						} catch (Exception e) {

						}
					}
					break;

				default:
					break;
			}
		}
		return null;
	}

	// Rule class to store robots rule information
	private static class Rule {
		final boolean allow;
		final String prefix;

		Rule(boolean allow, String prefix) {
			this.allow = allow;
			this.prefix = prefix;
		}
	}

	// Robots class to store robots rules
	private static class Robots {
		final List<Rule> rules = new ArrayList<>();
	}

	// parse robots rules
	private static Robots parseRobots(String robotsText, String ua) {
		Robots out = new Robots();
		if (robotsText == null) {
			return out;
		}

		String[] lines = robotsText.replace("\r", "").split("\n");
		List<String> currentUAs = new ArrayList<>();
		Map<String, List<Rule>> byUA = new LinkedHashMap<>();

		for (String raw : lines) {
			String line = raw.split("#", 2)[0].trim();
			if (line.isEmpty()) {
				continue;
			}

			int colon = line.indexOf(':');
			if (colon < 0) {
				continue;
			}
			String key = line.substring(0, colon).trim().toLowerCase(Locale.ROOT);
			String val = line.substring(colon + 1).trim();

			switch (key) {
				case "user-agent":
					if (currentUAs.isEmpty()) {
						currentUAs = new ArrayList<>();
					}
					currentUAs.add(val.toLowerCase(Locale.ROOT));
					byUA.putIfAbsent(val.toLowerCase(Locale.ROOT), new ArrayList<>());
					break;
				case "allow":
				case "disallow":
					if (currentUAs.isEmpty()) {
						break;
					}
					Rule rule = new Rule(key.equals("allow"), val);
					for (String u : currentUAs) {
						byUA.get(u).add(rule);
					}
					break;
				default:
					break;
			}

		}
		List<Rule> rules = byUA.get(ua.toLowerCase(Locale.ROOT));
		if (rules == null || rules.isEmpty()) {
			rules = byUA.get("*");
		}
		if (rules != null) {
			out.rules.addAll(rules);
		}
		return out;
	}

	// parse robots rules
	private static boolean parseRobotsRules(String robotsText, String path) {
		if (robotsText == null) {
			return true;
		}
		Robots r = parseRobots(robotsText, "cis5550-crawler");
		if (r.rules.isEmpty()) {
			return true;
		}
		for (Rule rule : r.rules) {
			String p;
			if (rule.prefix == null) {
				p = "";
			} else {
				p = rule.prefix;
			}

			if (p.isEmpty()) {
				if (!rule.allow) {
					return true;
				} else {
					continue;
				}
			}
			if (path.startsWith(p)) {
				return rule.allow;
			}
		}
		return true;
	}

	// load URL blacklist from file
	private static Set<String> loadUrlBlacklist(String filename) {
		Set<String> urls = new HashSet<>();
		try {
			File file = new File(filename);
			if (file.exists()) {
				try (BufferedReader br = new BufferedReader(new FileReader(file))) {
					String line;
					while ((line = br.readLine()) != null) {
						line = line.trim();
						if (!line.isEmpty() && !line.startsWith("#")) {
							String canon = canonicalize(line);
							if (canon != null) {
								urls.add(canon.toLowerCase(Locale.ROOT));
							}
						}
					}
				}
			}
		} catch (Exception e) {
		}
		return urls;
	}

	// check if content type is acceptable
	private static boolean isAcceptableContentType(String contentType) {
		if (contentType == null) {
			return true;
		}
		String ct = contentType.toLowerCase(Locale.ROOT);
		return ct.contains("text/html") || ct.contains("text/plain") || ct.contains("application/xhtml")
				|| ct.contains("text/xml") || ct.contains("application/xml");
	}

	// check if size is acceptable
	private static boolean isAcceptableSize(int size) {
		return size >= MIN_PAGE_SIZE && size <= MAX_PAGE_SIZE;
	}

	// check if content language is English
	private static boolean isEnglishLanguage(Map<String, String> headers) {
		// accept if no language header
		if (headers == null) {
			return true;
		}
		String contentLanguage = headers.get("content-language");
		if (contentLanguage == null) {
			return true;
		}
		contentLanguage = contentLanguage.toLowerCase(Locale.ROOT).trim();
		// accept if language starts with 'en'
		return contentLanguage.startsWith("en");
	}

	// check if URL matches calendar or archive patterns
	private static boolean isCalendarOrArchive(String url) {
		if (url == null || url.isEmpty()) {
			return false;
		}
		// extract path from URL (remove scheme, host, query string)
		try {
			URI uri = new URI(url);
			String path = uri.getPath();
			if (path == null || path.isEmpty()) {
				return false;
			}
			// check calendar pattern (YYYY/MM)
			if (CALENDAR_PATTERN.matcher(path).find()) {
				return true;
			}
			// check archive pattern (/archive, /calendar, /events, /schedule)
			if (ARCHIVE_PATTERN.matcher(path).find()) {
				return true;
			}
		} catch (Exception e) {
		}
		return false;
	}

	// Link class to store link information
	private static class Link {
		final String href;
		final String text;

		Link(String href, String text) {
			this.href = href;
			this.text = text;
		}
	}

	// clean anchor text
	private static String cleanAnchorText(String s) {
		if (s == null) {
			return "";
		}
		String noTags = s.replaceAll("(?is)<[^>]*>", " ");
		return noTags.replaceAll("\\s+", " ").trim();
	}

	// extract links from HTML
	private static List<Link> extractLinks(byte[] htmlBytes) {
		String html = new String(htmlBytes, StandardCharsets.UTF_8);
		List<Link> out = new ArrayList<>();
		Matcher m = A_TAG_RE.matcher(html);
		while (m.find()) {
			String attrs = m.group(1);
			String inner = cleanAnchorText(m.group(2));
			if (attrs == null) {
				continue;
			}

			Matcher h = HREF_RE.matcher(attrs);
			if (h.find()) {
				String val = h.group(2);
				if (val == null) {
					val = h.group(3);
				}
				if (val == null) {
					val = h.group(4);
				}
				if (val != null && !val.isEmpty()) {
					out.add(new Link(val, inner));
				}
			}
		}
		return out;
	}

	// extract URLs from HTML
	public static List<String> extractUrls(byte[] htmlBytes) {
		String html = new String(htmlBytes, StandardCharsets.UTF_8);
		List<String> out = new ArrayList<>();
		int i = 0;

		while (true) {
			int lt = html.indexOf('<', i);
			if (lt < 0) {
				break;
			}
			int gt = html.indexOf('>', lt + 1);
			if (gt < 0) {
				break;
			}

			String tagContent = html.substring(lt + 1, gt).trim();
			i = gt + 1;

			if (tagContent.startsWith("/")) {
				continue;
			}

			int space = -1;
			for (int k = 0; k < tagContent.length(); k++) {
				if (Character.isWhitespace(tagContent.charAt(k))) {
					space = k;
					break;
				}
			}

			String tag;
			if (space == -1) {
				tag = tagContent.toLowerCase();
			} else {
				tag = tagContent.substring(0, space).toLowerCase();
			}

			if (!"a".equals(tag)) {
				continue;
			}

			String attrs;
			if (space == -1) {
				attrs = "";
			} else {
				attrs = tagContent.substring(space + 1);
			}

			Matcher m = HREF_RE.matcher(attrs);
			if (m.find()) {
				String val = m.group(2);
				if (val == null) {
					val = m.group(3);
				}
				if (val == null) {
					val = m.group(4);
				}
				if (val != null && !val.isEmpty())
					out.add(val);
			}
		}
		return out;
	}

	// normalize the URL
	public static String normalizeUrl(String baseUrl, String rawHref) {
		if (rawHref == null) {
			return null;
		}
		String href = rawHref.trim();
		if (href.isEmpty()) {
			return null;
		}

		int hash = href.indexOf('#');
		if (hash >= 0) {
			href = href.substring(0, hash).trim();
			if (href.isEmpty()) {
				return null;
			}
		}

		try {
			URI base = new URI(baseUrl);
			if (href.startsWith("//")) {
				href = base.getScheme() + ":" + href;
			}
			URI hrefUri = new URI(href);
			URI resolved = base.resolve(hrefUri);

			String scheme = resolved.getScheme();
			if (scheme == null) {
				return null;
			}
			scheme = scheme.toLowerCase(Locale.ROOT);
			if (!scheme.equals("http") && !scheme.equals("https")) {
				return null;
			}

			String host = resolved.getHost();
			if (host == null || host.isEmpty()) {
				return null;
			}

			String path = resolved.getPath();
			if (path == null || path.isEmpty()) {
				path = "/";
			}
			String lowerPath = path.toLowerCase(Locale.ROOT);
			for (String ext : SKIP_EXT) {
				if (lowerPath.endsWith(ext)) {
					return null;
				}
			}
			String query = resolved.getQuery();
			String q;
			if (query == null) {
				q = "";
			} else {
				q = "?" + query;
			}

			int port = resolved.getPort();
			if (port == -1) {
				if (scheme.equals("https")) {
					port = 443;
				} else {
					port = 80;
				}
			}
			return scheme + "://" + host + ":" + port + path + q;
		} catch (Exception e) {
			return null;
		}
	}

	// canonicalize the URL
	private static String canonicalize(String rawUrl) {
		if (rawUrl == null) {
			return null;
		}
		int hash = rawUrl.indexOf('#');
		if (hash >= 0) {
			rawUrl = rawUrl.substring(0, hash);
		}

		String query = "";
		int q = rawUrl.indexOf('?');
		if (q >= 0) {
			query = rawUrl.substring(q);
			rawUrl = rawUrl.substring(0, q);
		}

		String[] p = URLParser.parseURL(rawUrl);
		if (p[0] == null || p[1] == null) {
			return null;
		}

		String scheme = p[0].toLowerCase(Locale.ROOT);
		String host = p[1].toLowerCase(Locale.ROOT);
		String port = p[2];
		String path = (p[3] == null || p[3].isEmpty()) ? "/" : p[3];

		if (port == null || port.isEmpty()) {
			port = scheme.equals("https") ? "443" : "80";
		}

		return scheme + "://" + host + ":" + port + path + query;
	}

	// CrawlOperation class to crawl and return links
	public static final class CrawlOperation implements FlameRDD.StringToIterable, Serializable {
		private final String kvsCoordinator;
		private final String blacklistTable;
		private transient Set<String> fileUrlBlacklist;
		private transient KVSClient kvs;
		private static final Object BLACKLIST_FILE_LOCK = new Object();
		private static final int BLACKLIST_FLUSH_INTERVAL = 50;
		private static final long BLACKLIST_FLUSH_TIME_MS = 60000L;
		private transient List<String> pendingBlacklistWrites;
		private transient long lastBlacklistFlushTime;

		private transient Map<String, Row> hostCache = Collections
				.synchronizedMap(new LinkedHashMap<String, Row>(1000, 0.75f, true) {
					protected boolean removeEldestEntry(Map.Entry eldest) {
						return size() > 1000;
					}
				});

		private transient Set<String> blacklistCache = Collections.synchronizedSet(new HashSet<>(10000));

		private transient Map<String, Long> hostDelayedUntil = Collections.synchronizedMap(new HashMap<>());

		// check if URL is blacklisted
		private boolean isUrlBlacklisted(KVSClient kvs, String urlHash) {
			if (blacklistCache.contains(urlHash)) {
				return true;
			}
			try {
				boolean exists = kvs.existsRow(BLACKLIST_TABLE, urlHash);
				if (exists) {
					blacklistCache.add(urlHash);
				}
				return exists;
			} catch (Exception e) {
				return false;
			}
		}

		// add URL to blacklist
		private void addToBlacklist(KVSClient kvs, String urlHash, String url, String reason) {
			try {
				Row blacklistRow = new Row(urlHash);
				blacklistRow.put("url", url);
				blacklistRow.put("reason", reason);
				blacklistRow.put("timestamp", Long.toString(System.currentTimeMillis()));
				kvs.putRow(BLACKLIST_TABLE, blacklistRow);
				blacklistCache.add(urlHash);
				bufferBlacklist(url, reason);
			} catch (Exception e) {
				System.err.println("Warning: Exception while blacklisting URL " + url + ": " + e.getMessage());
			}
		}

		// get robots text from host row
		private Row getRobots(KVSClient kvs, String scheme, String host, int port) {
			// check cache first
			Row cached = hostCache.get(host);
			if (cached != null && "1".equals(cached.get(ROBOTS_FETCH))) {
				return cached;
			}

			try {
				Row r = cached != null ? cached : kvs.getRow(HOSTS_TABLE, host);
				boolean fetched = (r != null && "1".equals(r.get(ROBOTS_FETCH)));
				if (fetched) {
					if (r != null) {
						hostCache.put(host, r);
					}
					return r;
				}

				String robotsUrl = scheme + "://" + host + ":" + port + "/robots.txt";
				HTTP.Response response = HTTP.doRequestWithTimeout("GET", robotsUrl, null, 2000, false);
				String text = "";
				if (response.statusCode() == 200) {
					byte[] body = response.body();
					text = new String(body, StandardCharsets.UTF_8);
				}
				Row w;
				if (r != null) {
					w = r;
				} else {
					w = new Row(host);
				}
				w.put(ROBOTS_TEXT, text);
				w.put(ROBOTS_FETCH, "1");
				Long delayMs = parseCrawlDelayMS(text, "cis5550-crawler");
				if (delayMs == null) {
					delayMs = parseCrawlDelayMS(text, "*");
				}
				if (delayMs != null) {
					w.put(CRAWL_DELAY, Long.toString(delayMs));
				}
				kvs.putRow(HOSTS_TABLE, w);
				hostCache.put(host, w);
				return w;
			} catch (Exception e) {
				String exceptionType = e.getClass().getName();
				String errorMsg = e.getMessage();
				Throwable cause = e.getCause();
				String causeType = cause != null ? cause.getClass().getName() : "";

				// check if it's a common failure
				boolean isCommonFailure = exceptionType.contains("ConnectException")
						|| exceptionType.contains("TimeoutException")
						|| exceptionType.contains("SocketTimeoutException")
						|| exceptionType.contains("UnknownHostException") || causeType.contains("ConnectException")
						|| causeType.contains("TimeoutException") || causeType.contains("SocketTimeoutException")
						|| causeType.contains("UnknownHostException") || causeType.contains("SocketException")
						|| (errorMsg != null && (errorMsg.contains("timed out") || errorMsg.contains("Connection")
								|| errorMsg.contains("Cannot connect")));

				if (!isCommonFailure) {
					System.err.println("Warning: Failed to fetch robots.txt for " + host + ": " + exceptionType + " - "
							+ errorMsg);
				}

				try {
					Row w = new Row(host);
					w.put(ROBOTS_TEXT, "");
					w.put(ROBOTS_FETCH, "1");
					kvs.putRow(HOSTS_TABLE, w);
					hostCache.put(host, w);
					return w;
				} catch (Exception er) {
					return null;
				}
			}
		}

		// update last access time
		private void updateLastAccess(KVSClient kvs, Row hostRow, String host, long now) {
			try {
				if (hostRow == null) {
					hostRow = hostCache.get(host);
					if (hostRow == null) {
						hostRow = new Row(host);
					}
				}
				hostRow.put(LAST_ACCESS, Long.toString(now));
				kvs.putRow(HOSTS_TABLE, hostRow);
				hostCache.put(host, hostRow);
			} catch (Exception e) {
			}
		}

		// buffer blacklist for file
		private void bufferBlacklist(String url, String reason) {
			try {
				if (pendingBlacklistWrites == null) {
					pendingBlacklistWrites = new ArrayList<>();
					lastBlacklistFlushTime = System.currentTimeMillis();
				}

				pendingBlacklistWrites.add("# " + reason + "\n" + url);

				long now = System.currentTimeMillis();
				boolean shouldFlush = pendingBlacklistWrites.size() >= BLACKLIST_FLUSH_INTERVAL
						|| (now - lastBlacklistFlushTime) >= BLACKLIST_FLUSH_TIME_MS;

				if (shouldFlush) {
					flushBlacklistToFile();
				}
			} catch (Exception e) {
			}
		}

		// flush blacklist to file
		private void flushBlacklistToFile() {
			synchronized (BLACKLIST_FILE_LOCK) {
				try {
					if (pendingBlacklistWrites == null || pendingBlacklistWrites.isEmpty()) {
						return;
					}

					File blacklistFile = new File("scrpit/blacklisted-urls.txt");
					boolean fileExists = blacklistFile.exists();

					try (BufferedWriter writer = new BufferedWriter(new FileWriter(blacklistFile, true))) {
						if (!fileExists) {
							writer.write("# Format: One URL per line.\\n");
							writer.write("#\n\n");
						}

						for (String entry : pendingBlacklistWrites) {
							writer.write(entry);
							writer.write("\n\n");
						}

						writer.flush();
					}

					pendingBlacklistWrites.clear();
					lastBlacklistFlushTime = System.currentTimeMillis();
				} catch (Exception e) {
				}
			}
		}

		// initialize the kvs coordinator and blacklist table
		public CrawlOperation(String kvsCoordinator, String blacklistTable) {
			this.kvsCoordinator = kvsCoordinator;
			this.blacklistTable = blacklistTable;
		}

		// extract links from HTML and add to nextUrls
		private void extractAndAddLinks(byte[] body, String canon, List<String> nextUrls, KVSClient kvs) {
			if (body == null || canon == null) {
				return;
			}
			List<Link> links = extractLinks(body);
			for (Link link : links) {
				String norm = normalizeUrl(canon, link.href);
				if (norm != null) {
					// skip calendar and archive URLs
					if (isCalendarOrArchive(norm)) {
						System.err.println("Skipping calendar/archive URL: " + norm);
						String urlHash = Hasher.hash(norm);
						addToBlacklist(kvs, urlHash, norm, "Calendar/Archive loop");
						continue;
					}
					nextUrls.add(norm);
				}
			}
		}

		// Crawl and returns links
		@Override
		public Iterable<String> op(String url) throws Exception {
			List<String> nextUrls = new ArrayList<>();

			if (url == null || url.isEmpty()) {
				return Collections.emptyList();
			}

			if (fileUrlBlacklist == null) {
				fileUrlBlacklist = loadUrlBlacklist("scrpit/blacklisted-urls.txt");
			}
			if (kvs == null) {
				kvs = new KVSClient(kvsCoordinator);
			}
			if (hostCache == null) {
				hostCache = Collections.synchronizedMap(new LinkedHashMap<String, Row>(1000, 0.75f, true) {
					protected boolean removeEldestEntry(Map.Entry eldest) {
						return size() > 1000;
					}
				});
			}
			if (blacklistCache == null) {
				blacklistCache = Collections.synchronizedSet(new HashSet<>(10000));
			}
			if (hostDelayedUntil == null) {
				hostDelayedUntil = Collections.synchronizedMap(new HashMap<>());
			}
			if (pendingBlacklistWrites == null) {
				pendingBlacklistWrites = new ArrayList<>();
				lastBlacklistFlushTime = System.currentTimeMillis();
			}

			// check from file
			// skip invalid or blacklisted URL
			String canon = canonicalize(url);
			if (canon == null) {
				return Collections.emptyList();
			}
			if (fileUrlBlacklist != null && fileUrlBlacklist.contains(canon.toLowerCase(Locale.ROOT))) {
				return Collections.emptyList();
			}

			String key = Hasher.hash(canon);

			// check from kvs table
			// skip blacklisted URL
			if (isUrlBlacklisted(kvs, key)) {
				return Collections.emptyList();
			}
			// skip duplicate URL
			try {
				if (kvs.existsRow("pt-crawl", key)) {
					return Collections.emptyList();
				}
			} catch (Exception e) {
			}

			URL u = new URL(canon);
			String host = u.getHost().toLowerCase(Locale.ROOT);
			String scheme = u.getProtocol().toLowerCase(Locale.ROOT);
			int port = (u.getPort() == -1 ? (scheme.equals("https") ? 443 : 80) : u.getPort());
			Row hostRow = getRobots(kvs, scheme, host, port);

			// check crawl delay
			long now = System.currentTimeMillis();
			if (needDelay(hostRow, now)) {
				// check if host is in delayed queue
				Long delayedUntil = hostDelayedUntil.get(host);
				long gapMs = readCrawlDelayMs(hostRow);
				if (delayedUntil == null || delayedUntil <= now) {
					long nextRetryTime = now + gapMs;
					hostDelayedUntil.put(host, nextRetryTime);
				}
				return Collections.emptyList();
			}
			hostDelayedUntil.remove(host);

			// get robots text from host row
			String robotsText = "";
			if (hostRow != null) {
				robotsText = hostRow.get(ROBOTS_TEXT);
				if (robotsText == null) {
					robotsText = "";
				}
			}
			String path = u.getPath();
			if (path == null || path.isEmpty()) {
				path = "/";
			}

			// check robots rules
			if (!parseRobotsRules(robotsText, path)) {
				return Collections.emptyList();
			}

			try {
				// get head first before entire body
				HTTP.Response headResponse = HTTP.doRequestWithTimeout("HEAD", canon, null, 800, true);
				int headCode = headResponse.statusCode();
				Map<String, String> headHeaders = headResponse.headers();
				String headType = headHeaders.get("content-type");
				String headLen = headHeaders.get("content-length");
				String headEncoding = headHeaders.get("content-encoding");

				if (headType != null) {
					int semi = headType.indexOf(';');
					if (semi >= 0) {
						headType = headType.substring(0, semi).trim();
					}
				}

				// check if language is English
				if (!isEnglishLanguage(headHeaders)) {
					Row r = new Row(key);
					r.put("url", canon);
					r.put("responseCode", Integer.toString(headCode));
					if (headType != null) {
						r.put("contentType", headType);
					}
					if (headLen != null) {
						r.put("length", headLen);
					}
					try {
						kvs.putRow("pt-crawl", r);
						updateLastAccess(kvs, hostRow, host, System.currentTimeMillis());
					} catch (Exception e) {
					}
					String language = headHeaders.get("content-language");
					if (language == null) {
						language = "unknown";
					}
					System.err.println("Skipping non-English page: " + canon + " (language: " + language + ")");
					addToBlacklist(kvs, key, canon, "Non-English language: " + language);
					return Collections.emptyList();
				}

				// handle redirects
				if (headCode >= 300 && headCode < 400) {
					Row r = new Row(key);
					r.put("url", canon);
					r.put("responseCode", Integer.toString(headCode));
					if (headType != null) {
						r.put("contentType", headType);
					}
					if (headLen != null) {
						r.put("length", headLen);
					}
					String loc = headHeaders.get("location");
					// extract redirect URL early before checking save success
					if (loc != null) {
						String norm = normalizeUrl(canon, loc);
						if (norm != null)
							nextUrls.add(norm);
					}
					try {
						kvs.putRow("pt-crawl", r);
						updateLastAccess(kvs, hostRow, host, System.currentTimeMillis());
					} catch (Exception e) {
						// retry the URL if save fails
						nextUrls.add(canon);
						return nextUrls;
					}
					return nextUrls;
				}

				// handle non-200 responses
				if (headCode != 200) {
					Row r = new Row(key);
					r.put("url", canon);
					r.put("responseCode", Integer.toString(headCode));
					if (headType != null) {
						r.put("contentType", headType);
					}
					if (headLen != null) {
						r.put("length", headLen);
					}
					try {
						kvs.putRow("pt-crawl", r);
						updateLastAccess(kvs, hostRow, host, System.currentTimeMillis());
					} catch (Exception e) {
						return List.of(canon);
					}
					// blacklist 404 URL
					if (headCode == 404) {
						System.err.println("Blacklisting 404 URL: " + canon);
						addToBlacklist(kvs, key, canon, "HTTP 404 Not Found");
					}
					return Collections.emptyList();
				}

				// check head content type
				if (headType != null && !isAcceptableContentType(headType)) {
					Row r = new Row(key);
					r.put("url", canon);
					r.put("responseCode", Integer.toString(headCode));
					r.put("contentType", headType);
					if (headLen != null) {
						r.put("length", headLen);
					}
					try {
						kvs.putRow("pt-crawl", r);
						updateLastAccess(kvs, hostRow, host, System.currentTimeMillis());
					} catch (Exception e) {
					}
					// blacklist invalid content type URL
					System.err.println("Blacklisting invalid content type URL: " + canon + " (type: " + headType + ")");
					addToBlacklist(kvs, key, canon, "Invalid content type: " + headType);
					return Collections.emptyList();
				}

				if (headLen != null) {
					try {
						long size = Long.parseLong(headLen);
						if (size > MAX_PAGE_SIZE) {
							Row r = new Row(key);
							r.put("url", canon);
							r.put("responseCode", Integer.toString(headCode));
							if (headType != null) {
								r.put("contentType", headType);
							}
							r.put("length", headLen);
							r.put("reason", "HEAD Content-Length too large: " + size);
							try {
								kvs.putRow("pt-crawl", r);
								updateLastAccess(kvs, hostRow, host, System.currentTimeMillis());
							} catch (Exception e) {
							}
							System.err.println("Skipping URL (HEAD Content-Length too large): " + canon + " (size: "
									+ size + ", encoding: " + (headEncoding != null ? headEncoding : "none") + ")");
							return Collections.emptyList();
						}
					} catch (NumberFormatException ei) {
					}
				}

				boolean isHtmlFromHead = headType != null && headType.toLowerCase(Locale.ROOT).contains("text/html");

				// get the entire body with timeout based on expected size
				int getTimeout = 5000;
				if (headLen != null) {
					try {
						long size = Long.parseLong(headLen);
						if (size < 500000) {
							getTimeout = 1000;
						} else if (size < 2000000) {
							getTimeout = 2000;
						}
					} catch (NumberFormatException e) {
					}
				}
				HTTP.Response getResponse = HTTP.doRequestWithTimeout("GET", canon, null, getTimeout, false);
				int resCode = getResponse.statusCode();

				Map<String, String> getHeaders = getResponse.headers();
				String getType = getHeaders.get("content-type");
				if (getType == null) {
					getType = headType;
				}
				if (getType != null) {
					int semi = getType.indexOf(';');
					if (semi >= 0) {
						getType = getType.substring(0, semi).trim();
					}
				}

				// handle GZIP decompression
				byte[] body;
				byte[] rawBody = getResponse.body();
				String enc = getHeaders.get("content-encoding");
				if (enc != null && enc.toLowerCase(Locale.ROOT).contains("gzip")) {
					try (ByteArrayInputStream bais = new ByteArrayInputStream(rawBody);
							GZIPInputStream gzis = new GZIPInputStream(bais);
							ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
						byte[] buf = new byte[20000];
						int n;
						while ((n = gzis.read(buf)) > 0) {
							baos.write(buf, 0, n);
						}
						body = baos.toByteArray();
					} catch (IOException e) {
						System.err.println(
								"GZIP decompression failed for " + canon + ": " + e.getMessage() + ", using raw body");
						body = rawBody;
					}
				} else {
					body = rawBody;
				}

				if (resCode != 200) {
					Row r = new Row(key);
					r.put("url", canon);
					r.put("responseCode", Integer.toString(resCode));
					if (getType != null) {
						r.put("contentType", getType);
					}
					r.put("length", Integer.toString(body.length));
					try {
						kvs.putRow("pt-crawl", r);
						updateLastAccess(kvs, hostRow, host, System.currentTimeMillis());
					} catch (Exception e) {
					}
					return Collections.emptyList();
				}

				if (!isAcceptableSize(body.length)) {
					// save to pt-crawl before blacklisting
					Row r = new Row(key);
					r.put("url", canon);
					r.put("responseCode", Integer.toString(resCode));
					if (getType != null) {
						r.put("contentType", getType);
					}
					r.put("length", Integer.toString(body.length));
					try {
						kvs.putRow("pt-crawl", r);
						updateLastAccess(kvs, hostRow, host, System.currentTimeMillis());
					} catch (Exception e) {
					}
					// blacklist invalid body size URL
					System.err.println("Blacklisting invalid body size URL: " + canon + " (size: " + body.length
							+ ", raw: " + rawBody.length + ")");
					addToBlacklist(kvs, key, canon, "Invalid body size: " + body.length);
					return Collections.emptyList();
				}

				// save content to kvs table
				Row r = new Row(key);
				r.put("url", canon);
				r.put("responseCode", "200");
				r.put("length", Integer.toString(body.length));
				r.put("contentType", getType);

				boolean isHtml = getType.toLowerCase(Locale.ROOT).contains("text/html");

				String contentHash = null;
				Row seen = null;
				if (isHtml) {
					contentHash = Hasher.hash(new String(body, StandardCharsets.UTF_8));
					try {
						seen = kvs.getRow(CONTENT_TABLE, contentHash);
					} catch (Exception e) {
					}
				}

				// save new content
				if (isHtml && seen == null) {
					Row contentRow = new Row(contentHash);
					contentRow.put("url", canon);
					try {
						kvs.putRow(CONTENT_TABLE, contentRow);
					} catch (Exception e) {
						// if content save fails
						r.put("page", body);
						extractAndAddLinks(body, canon, nextUrls, kvs);
						// add current URL for retry
						nextUrls.add(canon);
						return nextUrls;
					}

					r.put("page", body);
					extractAndAddLinks(body, canon, nextUrls, kvs);
				} else if (isHtml && seen != null) {
					// if content already exists
					r.put(CANONICAL_COL, seen.get("url"));
					extractAndAddLinks(body, canon, nextUrls, kvs);
				} else if (isHtml) {
					r.put("page", body);
					extractAndAddLinks(body, canon, nextUrls, kvs);
				}

				// save row to kvs table
				try {
					kvs.putRow("pt-crawl", r);
					updateLastAccess(kvs, hostRow, host, System.currentTimeMillis());
				} catch (Exception e) {
					nextUrls.add(canon);
					return nextUrls;
				}

			} catch (Exception e) {
				// log errors
				String errorMsg = e.getMessage();
				String exceptionType = e.getClass().getName();
				System.err.println("Error crawling " + url + ": " + exceptionType + " - " + errorMsg);

				Throwable cause = e.getCause();
				String causeType = cause != null ? cause.getClass().getName() : "";
				String causeMsg = cause != null ? cause.getMessage() : "";

				// check UnknownHostException
				String stackTrace = "";
				try {
					java.io.StringWriter sw = new java.io.StringWriter();
					java.io.PrintWriter pw = new java.io.PrintWriter(sw);
					e.printStackTrace(pw);
					stackTrace = sw.toString();
				} catch (Exception ex) {
				}

				// check for permanent failures
				boolean isPermanentFailure = exceptionType.contains("UnknownHostException")
						|| exceptionType.contains("NoRouteToHostException")
						|| causeType.contains("UnknownHostException") || causeType.contains("NoRouteToHostException")
						|| stackTrace.contains("UnknownHostException")
						|| (errorMsg != null && errorMsg.contains("Name or service not known"))
						|| (causeMsg != null && causeMsg.contains("Name or service not known"));

				// check for transient errors
				boolean isTransientError = !isPermanentFailure && ((exceptionType.contains("SocketTimeoutException")
						|| exceptionType.contains("TimeoutException"))
						|| (causeType.contains("SocketTimeoutException") || causeType.contains("TimeoutException"))
						|| (exceptionType.contains("SocketException")
								&& !exceptionType.contains("UnknownHostException"))
						|| (causeType.contains("SocketException") && !causeType.contains("UnknownHostException"))
						|| (errorMsg != null
								&& (errorMsg.contains("Read timed out") || errorMsg.contains("Connection reset")
										|| errorMsg.contains("Connection timed out") || errorMsg.contains("timeout")
										|| errorMsg.contains("PKIX") || errorMsg.contains("ConnectException")))
						|| (causeMsg != null && (causeMsg.contains("Read timed out") || causeMsg.contains("timeout")
								|| causeMsg.contains("Connection reset"))));

				if (isPermanentFailure) {
					System.err.println(
							"Blacklisting permanently failed URL: " + canon + " (reason: " + exceptionType + ")");
					addToBlacklist(kvs, key, canon, "Permanent failure: " + exceptionType);
					return Collections.emptyList();
				}

				// retry if transient error
				if (nextUrls.isEmpty() && isTransientError) {
					nextUrls.add(canon);
				}
			}
			return nextUrls;
		}
	}

	public static void run(FlameContext ctx, String[] args) throws Exception {
		if (args.length == 0 || args[0] == null || args[0].isEmpty()) {
			ctx.output("Missing URL in arg[0].");
			return;
		}
		// set seeds
		List<String> seedURLs = new ArrayList<>();
		for (String arg : args) {
			if (arg != null && !arg.isEmpty()) {
				seedURLs.add(arg);
			}
		}
		if (seedURLs.isEmpty()) {
			ctx.output("No valid seed URLs provided.");
			return;
		}

		final String kvsCoordinator = ctx.getKVS().getCoordinator();
		final String blacklistTable = null;
		final String URL_QUEUE_TABLE = "pt-crawler-url-queue";

		KVSClient kvs = ctx.getKVS();
		FlameRDD urlQueue = null;
		int iteration = 0;

		// try to resume from checkpoint
		try {
			urlQueue = ctx.fromTable(URL_QUEUE_TABLE, row -> row.get("value"));
			int checkpointSize = urlQueue.count();
			if (checkpointSize > 0) {
				ctx.output("Resuming from checkpoint with " + checkpointSize + " URLs in queue");
				System.err.println("CRAWLER: Resumed from checkpoint table: " + URL_QUEUE_TABLE + " with "
						+ checkpointSize + " URLs");
			} else {
				throw new Exception("Empty checkpoint, starting fresh");
			}
		} catch (Exception e) {
			ctx.output("Starting fresh crawl with " + seedURLs.size() + " seed URLs");
			System.err.println("CRAWLER: Starting fresh crawl - " + e.getMessage());
			urlQueue = ctx.parallelize(seedURLs);
		}

		final CrawlOperation crawler = new CrawlOperation(kvsCoordinator, blacklistTable);
		final int BASE_SAVE_INTERVAL = 5;
		long nextCheckpointIteration = BASE_SAVE_INTERVAL;
		int checkpointIntervalMultiplier = 1;
		final int MAX_ITERATIONS = Integer.MAX_VALUE;
		long lastCheckpointTime = System.currentTimeMillis();
		final long CHECKPOINT_TIME_INTERVAL_MS = 15 * 60 * 1000;
		int consecutiveCheckpointFailures = 0;
		final int MAX_CHECKPOINT_FAILURES = 3;

		// crawling the queue
		while (iteration < MAX_ITERATIONS) {
			iteration++;
			long iterationStartTime = System.currentTimeMillis();
			System.err.println("CRAWLER: Starting iteration " + iteration);
			urlQueue = urlQueue.flatMap(crawler);
			long iterationEndTime = System.currentTimeMillis();
			long iterationDuration = iterationEndTime - iterationStartTime;
			System.err.println(
					"CRAWLER: Finished iteration " + iteration);

			boolean shouldCheckpoint = false;
			if (iteration == 1 || iteration % 100 == 0) {
				try {
					long queueSize = urlQueue.count();
					System.err.println("CRAWLER: Queue size after iteration " + iteration + ": " + queueSize);
					if (queueSize == 0) {
						ctx.output("URL queue is empty (iteration " + iteration + "), crawl complete");
						try {
							urlQueue.saveAsTable(URL_QUEUE_TABLE);
							ctx.output("Final checkpoint saved");
						} catch (Exception e) {
							ctx.output("Warning: Failed to save final checkpoint: " + e.getMessage());
						}
						break;
					}
				} catch (Exception e) {
					ctx.output("Warning: Failed to check queue size: " + e.getMessage());
				}
			}

			long now = System.currentTimeMillis();
			long timeSinceLastCheckpoint = now - lastCheckpointTime;

			// checkpoint at first iteration
			if (iteration == 1) {
				shouldCheckpoint = true;
			} else if (iteration >= nextCheckpointIteration) {
				shouldCheckpoint = true;
			}

			if (timeSinceLastCheckpoint >= CHECKPOINT_TIME_INTERVAL_MS) {
				shouldCheckpoint = true;
			}

			if (shouldCheckpoint) {
				System.err.println("CRAWLER: Checkpointing at iteration " + iteration);
				try {
					long queueSize = urlQueue.count();

					if (queueSize == 0) {
						System.err.println("CRAWLER: Skipping checkpoint - queue is empty");
						ctx.output("Warning: Skipping checkpoint at iteration " + iteration + " - queue is empty");
						lastCheckpointTime = now;
						nextCheckpointIteration = iteration + (BASE_SAVE_INTERVAL * checkpointIntervalMultiplier);
					} else {
						long checkpointStart = System.currentTimeMillis();
						kvs.delete(URL_QUEUE_TABLE);
						urlQueue.saveAsTable(URL_QUEUE_TABLE);
						long checkpointDuration = System.currentTimeMillis() - checkpointStart;

						long savedCount = 0;
						try {
							savedCount = kvs.count(URL_QUEUE_TABLE);
							System.err.println("CRAWLER: Checkpoint saved (iteration " + iteration + ", " + savedCount
									+ " rows)");
							ctx.output("Checkpoint saved (iteration " + iteration + ", " + savedCount + " rows)");

							if (savedCount == 0) {
								System.err.println("CRAWLER: WARNING - Checkpoint table is empty after save!");
								ctx.output("WARNING: Checkpoint table is empty after save operation!");
							} else if (savedCount != queueSize) {
								System.err.println("CRAWLER: WARNING - Saved row count (" + savedCount
										+ ") doesn't match queue size (" + queueSize + ")");
								ctx.output("WARNING: Saved row count (" + savedCount + ") doesn't match queue size ("
										+ queueSize + ")");
							}
						} catch (Exception verifyEx) {
							System.err.println(
									"CRAWLER: WARNING - Failed to verify checkpoint: " + verifyEx.getMessage());
							ctx.output("WARNING: Failed to verify checkpoint was saved: " + verifyEx.getMessage());
						}

						lastCheckpointTime = now;
						consecutiveCheckpointFailures = 0;

						if (checkpointDuration > 30000) {
							// max 6x (90k iterations)
							checkpointIntervalMultiplier = Math.min(checkpointIntervalMultiplier * 2, 6);
							ctx.output("Checkpoint took too long, increasing interval multiplier to "
									+ checkpointIntervalMultiplier);
						} else if (checkpointDuration < 5000 && checkpointIntervalMultiplier > 1) {
							checkpointIntervalMultiplier = Math.max(1, checkpointIntervalMultiplier / 2);
						}
						if (iteration == 1) {
							nextCheckpointIteration = BASE_SAVE_INTERVAL;
						} else {
							nextCheckpointIteration = iteration + (BASE_SAVE_INTERVAL * checkpointIntervalMultiplier);
						}
					}

				} catch (Exception e) {
					consecutiveCheckpointFailures++;
					System.err.println("CRAWLER: Exception during checkpoint: " + e.getMessage());
					e.printStackTrace();
					ctx.output("Warning: Failed to save checkpoint (attempt " + consecutiveCheckpointFailures + "): "
							+ e.getMessage());

					if (consecutiveCheckpointFailures >= 2) {
						lastCheckpointTime = System.currentTimeMillis();
						System.err.println(
								"CRAWLER: Updating lastCheckpointTime after repeated failures to prevent blocking");
					}

					if (consecutiveCheckpointFailures >= MAX_CHECKPOINT_FAILURES) {
						// max 10x (150k iterations)
						checkpointIntervalMultiplier = Math.min(checkpointIntervalMultiplier * 3, 10);
						ctx.output("Multiple checkpoint failures detected, increasing interval multiplier to "
								+ checkpointIntervalMultiplier + " to reduce KVS load");
						consecutiveCheckpointFailures = 0; // reset counter
					}

					nextCheckpointIteration = iteration + (BASE_SAVE_INTERVAL * checkpointIntervalMultiplier);
				}
			}
		}

		// update checkpoint when manually stopped
		try {
			long finalQueueSize = urlQueue.count();
			if (finalQueueSize > 0) {
				System.err.println("CRAWLER: Saving final checkpoint with " + finalQueueSize + " URLs");
				urlQueue.saveAsTable(URL_QUEUE_TABLE);
				ctx.output("Final checkpoint saved with " + finalQueueSize + " URLs");
			} else {
				System.err.println("CRAWLER: Skipping final checkpoint - queue is empty");
			}
		} catch (Exception e) {
			System.err.println("CRAWLER: Failed to save final checkpoint: " + e.getMessage());
			e.printStackTrace();
		}
	}

}
