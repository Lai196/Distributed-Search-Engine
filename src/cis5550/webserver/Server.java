package cis5550.webserver;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.*;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;

import javax.net.ServerSocketFactory;
import javax.net.ssl.*;
import cis5550.tools.*;
import java.security.KeyStore;

public class Server implements Runnable {

	private static final int NUM_WORKERS = 100;
	private static final BlockingQueue<Socket> QUEUE = new LinkedBlockingQueue<Socket>();
	private static final DateTimeFormatter RFC1123 = DateTimeFormatter.RFC_1123_DATE_TIME;
	private static Server INSTANCE = null;
	private static boolean launch = false;
	private int portNumber = 0;
	private int securePort = 0;
	private String filesLocation = null;
	private static final String DEFAULT_HOST = "__default__";
	private static final ConcurrentHashMap<String, HostConfig> HOSTS = new ConcurrentHashMap<>();
	private static volatile String CURRENT_HOST = null;
	private final String pwd = "secret";
	private static final ConcurrentHashMap<String, SessionImpl> SESSIONS = new ConcurrentHashMap<>();
	private static final Random RANDOM = new Random();

	public void run() {

		final int port = this.portNumber;
		final String fileDir = this.filesLocation;
		HostConfig def = getHostConfig();
		if (def.jksPath == null) {
			def.jksPath = "keystore.jks";
			def.jksPass = "secret";
		}

		if (port > 0) {
			new Thread(() -> {
				try {
					ServerSocket ssock = new ServerSocket(port);

					serverLoop(ssock);
				} catch (Exception e) {

				}
			}, "http-acceptor").start();
		}

		if (securePort > 0) {
			new Thread(() -> {
				try (ServerSocket ssock = new ServerSocket(securePort)) {
					while (true) {
						Socket sock = ssock.accept();
						sock.setTcpNoDelay(true);

						try {

							SNIInspector sni = new SNIInspector();
							sni.parseConnection(sock);
							String sniHost = null;
							if (sni.getHostName() != null) {
								sniHost = sni.getHostName().getAsciiName().toLowerCase();
							}

							SSLContext sslContext;
							if (sniHost != null && HOSTS.containsKey(hostKey(sniHost))) {
								sslContext = contextForHost(sniHost);
							} else {
								sslContext = contextForHost(null);
							}

							SSLSocketFactory sf = sslContext.getSocketFactory();
							SSLSocket sslSock = (SSLSocket) sf.createSocket(sock, sni.getInputStream(), true);

							QUEUE.put(sslSock);

						} catch (Exception e) {
							try {
								sock.close();
							} catch (Exception ex) {

							}
						}
					}
				} catch (Exception e) {

				}
			}, "https-acceptor").start();
		}

		for (int i = 0; i < NUM_WORKERS; i++) {
			String name = "worker" + i;
			new Thread(() -> {
				while (true) {
					Socket sock;
					try {
						sock = QUEUE.take();
					} catch (InterruptedException ie) {
						return;
					}
					clientConnection(sock, fileDir);
				}
			}, name).start();
		}

		removeExpiredSession();
	}

	private static void clientConnection(Socket socket, String fileDir) {
		try {
			InputStream in = new BufferedInputStream(socket.getInputStream());
			OutputStream out = new BufferedOutputStream(socket.getOutputStream());

			byte[] headerEnd = new byte[] { 13, 10, 13, 10 };
			byte[] buf = new byte[1000];

			while (true) {
				ByteArrayOutputStream headerBytes = new ByteArrayOutputStream();
				int match = 0;

				while (true) {
					int b = in.read();
					if (b == -1) {
						return;
					}
					headerBytes.write(b);

					if (b == headerEnd[match]) {
						match++;
						if (match == 4) {
							break;
						}
					} else {
						match = 0;
					}
				}

				BufferedReader br = new BufferedReader(
						new InputStreamReader(new ByteArrayInputStream(headerBytes.toByteArray())));

				String requestLine = br.readLine();
				if (requestLine == null || requestLine.isEmpty()) {
					sendError(out, 400, "Bad Request");
					dispose(in, 0, buf);
					continue;
				}

				String[] parts = requestLine.split(" ");

				if (parts.length != 3) {
					sendError(out, 400, "Bad Request");
					out.flush();
					dispose(in, 0, buf);
					continue;
				}

				String method = parts[0];
				String URL = parts[1];
				String version = parts[2];

				String line;
				String ims = null;
				String rangeLine = null;
				boolean hasHost = false;
				int contentLength = 0;
				Map<String, String> headerMap = new LinkedHashMap<>();

				while ((line = br.readLine()) != null && !line.isEmpty()) {
					int semiIdx = line.indexOf(':');
					if (semiIdx < 0) {
						continue;
					}
					String name = line.substring(0, semiIdx).trim().toLowerCase();
					String val = line.substring(semiIdx + 1).trim();
					if ("host".equals(name)) {
						hasHost = true;
					} else if ("content-length".equals(name)) {
						try {
							contentLength = Integer.parseInt(val);
							if (contentLength < 0)
								contentLength = 0;
						} catch (NumberFormatException e) {

						}
					} else if ("if-modified-since".equals(name)) {
						ims = val;
					} else if ("range".equals(name)) {
						rangeLine = val;
					}

					if (headerMap.containsKey(name)) {
						String oldVal = headerMap.get(name);
						String newVal = oldVal + "," + val;
						headerMap.put(name, newVal);
					} else {
						headerMap.put(name, val);
					}
				}

				String cookieHeader = headerMap.get("cookie");
				String sessionCookieId = null;
				if (cookieHeader != null) {
					String[] cookies = cookieHeader.split(";");
					for (String cookie : cookies) {
						String[] k = cookie.trim().split("=", 2);
						if (k.length == 2 && k[0].equals("SessionID")) {
							sessionCookieId = k[1];
						}
					}
				}

				SessionImpl session = null;
				if (sessionCookieId != null) {
					session = SESSIONS.get(sessionCookieId);
					if (session != null && session.isValid()) {
						session.access();
					} else {
						session = null;
					}
				}

				String hostHeader = headerMap.get("host");
				HostConfig hostCfg = getReqHost(hostHeader);

				CopyOnWriteArrayList<routeData> activeRoutes;
				if (hostCfg != null) {
					activeRoutes = hostCfg.routes;
				} else {
					activeRoutes = null;
				}

				String activeStaticDir;
				if (hostCfg != null && hostCfg.staticEnabled) {
					activeStaticDir = hostCfg.staticDir;
				} else {
					activeStaticDir = null;
				}

				CopyOnWriteArrayList<Route> beforeFilters;
				if (hostCfg != null) {
					beforeFilters = hostCfg.beforeFilters;
				} else {
					beforeFilters = null;
				}

				CopyOnWriteArrayList<Route> afterFilters;
				if (hostCfg != null) {
					afterFilters = hostCfg.afterFilters;
				} else {
					afterFilters = null;
				}

				byte[] bodyRaw = new byte[0];
				if (contentLength > 0) {
					bodyRaw = new byte[contentLength];
					int start = 0;
					while (start < contentLength) {
						int r = in.read(bodyRaw, start, contentLength - start);
						if (r == -1) {
							break;
						}
						start += r;
					}
				}

				if (!"HTTP/1.1".equals(version)) {
					sendError(out, 505, "HTTP Version Not Supported");
					out.flush();
					dispose(in, contentLength, buf);
					continue;
				}

				if (!hasHost) {
					sendError(out, 400, "Bad Request");
					out.flush();
					dispose(in, contentLength, buf);
					continue;
				}

				if (!"GET".equalsIgnoreCase(method) && !"HEAD".equalsIgnoreCase(method)
						&& !"POST".equalsIgnoreCase(method) && !"PUT".equalsIgnoreCase(method)) {
					sendError(out, 501, "Not Implemented");
					out.flush();
					dispose(in, contentLength, buf);
					continue;
				}

				if (URL.contains("..")) {
					sendError(out, 403, "Forbidden");
					out.flush();
					dispose(in, contentLength, buf);
					continue;
				}

				String URLpath = URL;
				String queryStr = null;
				int queryIdx = URL.indexOf('?');
				if (queryIdx >= 0) {
					URLpath = URL.substring(0, queryIdx);
					queryStr = URL.substring(queryIdx + 1);
				}

				Map<String, String> queryParams = new LinkedHashMap<>();

				if (queryStr != null) {
					try {
						parseQueryParams(queryStr, queryParams);
					} catch (Exception e) {

					}
				}

				String contentHeader = headerMap.get("content-type");
				if (contentHeader != null
						&& contentHeader.toLowerCase().startsWith("application/x-www-form-urlencoded")) {
					if (bodyRaw != null && bodyRaw.length > 0) {
						try {
							String form = new String(bodyRaw, StandardCharsets.UTF_8);
							parseQueryParams(form, queryParams);
						} catch (Exception e) {

						}
					}
				}

				routeData matchedRoute = null;
				Map<String, String> pathParams = null;

				if (activeRoutes != null) {
					for (routeData r : activeRoutes) {
						if (!r.method.equalsIgnoreCase(method)) {
							continue;
						}
						Map<String, String> m = matchParams(r.pattern, URLpath);
						if (m != null) {
							matchedRoute = r;
							pathParams = m;
							break;
						}
					}
				}

				if (matchedRoute != null) {

					RequestImpl req = new RequestImpl(method, URLpath, version, headerMap, queryParams, pathParams,
							new InetSocketAddress(socket.getInetAddress(), socket.getPort()), bodyRaw, INSTANCE);
					if (session != null) {
						req.setSession(session);
					}

					ResponseImpl res = new ResponseImpl();
					res.setOutputStream(out);
					res.setHead("HEAD".equalsIgnoreCase(method));

					if (beforeFilters != null) {
						for (Route f : beforeFilters) {
							try {
								f.handle(req, res);
								if (res.isHalted()) {
									sendError(out, res.getHaltCode(), res.getHaltReason());
									out.flush();
									dispose(in, 0, buf);
									return;
								}
							} catch (Exception e) {
								sendError(out, 500, "Internal Server Error");
								out.flush();
								dispose(in, 0, buf);
								return;
							}
						}
					}

					Object handlerResult = null;
					try {
						handlerResult = matchedRoute.handler.handle(req, res);
					} catch (Exception e) {
						System.err.println("Error handling request: " + method + " " + URLpath);
						e.printStackTrace();
						if (!res.isWriteStatus()) {
							sendError(out, 500, "Internal Server Error");
							out.flush();
						}
						dispose(in, 0, buf);
						continue;
					}

					if (afterFilters != null) {
						for (Route f : afterFilters) {
							try {
								f.handle(req, res);
							} catch (Exception e) {
							}
						}
					}

					if (!res.isWriteStatus()) {
						byte[] resBody = res.getBody();
						if (resBody == null) {
							if (handlerResult != null) {
								resBody = handlerResult.toString().getBytes();
							} else {
								resBody = null;
							}
						}

						int resBodyLen;
						if (resBody == null) {
							resBodyLen = 0;
						} else {
							resBodyLen = resBody.length;
						}

						String contentType = res.getContentType();

                        String header =
                        	    "HTTP/1.1 " + res.getStatusCode() + " " + res.getStatusReason() + "\r\n" 
                        	    + "Server: AskSauron\r\n" 
                        	    + "Connection: close\r\n" 
                        	    + "Content-Type: " + contentType + "\r\n" 
                        	    + "Content-Length: " + resBodyLen + "\r\n";

						for (Map.Entry<String, List<String>> k : res.getHeaders().entrySet()) {
							String headerName = k.getKey();
							for (String headerVal : k.getValue()) {
								header = header + headerName + ": " + headerVal + "\r\n";
							}
						}

						boolean isHttps = (socket instanceof SSLSocket);
						if (req instanceof RequestImpl && ((RequestImpl) req).isNewSessionCreated()) {
							String sid = ((Session) req.session()).id();
							String cookie = "Set-Cookie: SessionID=" + sid + "; Path=/" + "; HttpOnly"
									+ "; SameSite=Lax";
							if (isHttps) {
								cookie += "; Secure";
							}

							cookie += "\r\n";
							header += cookie;
						}

						header += "\r\n";

						out.write(header.getBytes());
						if (!"HEAD".equalsIgnoreCase(method) && resBodyLen > 0) {
							out.write(resBody);
						}
						out.flush();
						dispose(in, 0, buf);
						break;

					} else {
						out.flush();
						dispose(in, 0, buf);
						break;

					}
				}

				if (activeStaticDir == null) {
					sendError(out, 404, "Not Found");
					out.flush();
					dispose(in, contentLength, buf);
					continue;
				}

				String filePath = activeStaticDir + URLpath;

				File file = new File(filePath);
				if (!file.exists()) {
					sendError(out, 404, "Not Found");
					out.flush();
					dispose(in, contentLength, buf);
					continue;
				}
				if (!file.canRead()) {
					sendError(out, 403, "Forbidden");
					out.flush();
					dispose(in, contentLength, buf);
					continue;
				}

				long imsMilli = -1;
				if (ims != null) {
					try {
						ZonedDateTime zdt = ZonedDateTime.parse(ims, DateTimeFormatter.RFC_1123_DATE_TIME);
						imsMilli = zdt.toInstant().toEpochMilli();
					} catch (DateTimeParseException e) {

					}
				}

				long fileMilli = file.lastModified();
				String lastMod = DateTimeFormatter.RFC_1123_DATE_TIME
						.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(fileMilli), ZoneOffset.UTC));

				if (imsMilli >= 0) {
					if ((fileMilli / 1000) <= (imsMilli / 1000)) {
						String header = "HTTP/1.1 304 Not Modified\r\n" + "Server: SecondTry\r\n" + "Last-Modified: "
								+ lastMod + "\r\n" + "\r\n";
						out.write(header.getBytes());
						out.flush();
						dispose(in, contentLength, buf);
						continue;
					}
				}
				long length = file.length();

				long[] ranges = parseRange(rangeLine, length);

				if (ranges != null) {
					long start = ranges[0];
					long end = ranges[1];
					long partSize = (end - start + 1);

					String contentType = getContentType(filePath);
                    String headers =
                            "HTTP/1.1 206 Partial Content\r\n"
                            + "Content-Type: " + contentType + "\r\n"
                            + "Server: AskSauron\r\n"
                            + "Accept-Ranges: bytes\r\n"
                            + "Last-Modified: " + lastMod + "\r\n"
                            + "Content-Range: bytes " + start + "-" + end + "/" + length + "\r\n"
                            + "Content-Length: " + partSize + "\r\n"
                            + "\r\n";
                    out.write(headers.getBytes());

					if ("GET".equalsIgnoreCase(method)) {
						try (InputStream targetFile = new BufferedInputStream(new FileInputStream(file))) {
							long skipped = targetFile.skip(start);
							while (skipped < start) {
								long s = targetFile.skip(start - skipped);
								if (s <= 0) {
									break;
								}
								skipped += s;
							}
							long remaining = end - start + 1;
							int r;
							while (remaining > 0
									&& (r = targetFile.read(buf, 0, (int) Math.min(buf.length, remaining))) != -1) {
								out.write(buf, 0, r);
								remaining -= r;
							}
						}
					}
					out.flush();
					dispose(in, contentLength, buf);
					continue;

				}

				String contentType = getContentType(filePath);
                String header =
                		"HTTP/1.1 200 OK\r\n"
                		+ "Content-Type: " + contentType + "\r\n"
                		+ "Server: AskSauron\r\n"
                		+ "Last-Modified: " + lastMod + "\r\n"
                        + "Content-Length: " + length + "\r\n"
                        + "\r\n";
				out.write(header.getBytes());

				if ("GET".equalsIgnoreCase(method)) {
					try (InputStream targetFile = new BufferedInputStream(new FileInputStream(file))) {
						int r;
						while ((r = targetFile.read(buf)) != -1) {
							out.write(buf, 0, r);
						}
					}
				}
				out.flush();
				dispose(in, contentLength, buf);

			}
		} catch (IOException e) {

		} finally {
			try {
				socket.close();
			} catch (IOException e) {

			}
		}

	}

	private static void sendError(OutputStream out, int code, String note) throws IOException {
		String body = code + " " + note + "\r\n";
		String headers = "HTTP/1.1 " + code + " " + note + "\r\n" 
						+ "Content-Type: text/plain\r\n"
						+ "Server: AskSauron\r\n"
						+ "Content-Length: " + body.getBytes().length + "\r\n" 
						+ "\r\n";
		out.write(headers.getBytes());
		out.write(body.getBytes());
	}

	static void dispose(InputStream in, int contentLength, byte[] buf) throws IOException {
		int dispose = contentLength;
		while (dispose > 0) {
			int n = Math.min(dispose, buf.length);
			int r = in.read(buf, 0, n);
			if (r == -1)
				break;
			dispose -= r;
		}
	}

	private static String getContentType(String filePath) {

		if (filePath == null) {                         // default to octet-stream
			return "application/octet-stream";
		}
		String lower = filePath.toLowerCase();                                                 // cast filename to lowercase and grab extension
		String ext = lower.contains(".") ? lower.substring(lower.lastIndexOf('.') + 1) : "";
		
		String type = switch (ext) {

			case "html", "htm" -> "text/html";
			case "jpg", "jpeg" -> "image/jpeg";
			case "png" -> "image/png";
			case "ico" -> "image/x-icon";
			case "txt" -> "text/plain";
			case "css" -> "text/css";
			case "js" -> "application/javascript";
			case "pdf" -> "application/pdf";

			default -> "application/octet-stream";      // default to octet-stream
		};
    
		System.out.println("getContentType(" + filePath + ") -> ext=" + ext + ", type=" + type);
    
		return type;
	}

	private static long[] parseRange(String rangeLine, long length) {
		if (rangeLine == null || length <= 0) {
			return null;
		}
		String line = rangeLine.trim().toLowerCase();
		if (!line.startsWith("bytes=")) {
			return null;
		}

		String ranges = line.substring(6).trim();

		try {
			long start = -1;
			long end = -1;
			long range1;
			long range2;

			if (ranges.startsWith("-")) {
				range2 = Long.parseLong(ranges.substring(1).trim());
				if (range2 <= 0) {
					return null;
				}
				if (range2 >= length) {
					start = 0;
					end = length - 1;
				} else {
					start = length - range2;
					end = length - 1;
				}
			} else if (ranges.endsWith("-")) {
				range1 = Long.parseLong(ranges.substring(0, ranges.length() - 1).trim());
				if (range1 < 0 || range1 >= length) {
					return null;
				}
				start = range1;
				end = length - 1;
			} else if (ranges.contains("-")) {
				range1 = Long.parseLong(ranges.substring(0, ranges.indexOf('-')).trim());
				range2 = Long.parseLong(ranges.substring(ranges.indexOf('-') + 1).trim());
				if (range1 < 0 || range2 < 0 || range1 > range2 || range1 >= length) {
					return null;
				}
				start = range1;
				if (range2 >= length) {
					end = length - 1;
				} else {
					end = range2;
				}
			}
			if (start != -1 && end != -1) {
				return new long[] { start, end };
			} else {
				return null;
			}

		} catch (NumberFormatException e) {
			return null;
		}
	}

	public static class staticFiles {
		public static void location(String s) {
			checkInstance();
			HostConfig cfg = getHostConfig();
			cfg.staticDir = s;
			cfg.staticEnabled = true;
			checkStarted();
		}
	}

	public static void get(String path, Route route) {
		checkInstance();
		HostConfig cfg = getHostConfig();
		cfg.routes.add(new routeData("GET", path, route));
		checkStarted();
	}

	public static void post(String path, Route route) {
		checkInstance();
		HostConfig cfg = getHostConfig();
		cfg.routes.add(new routeData("POST", path, route));
		checkStarted();
	}

	public static void put(String path, Route route) {
		checkInstance();
		HostConfig cfg = getHostConfig();
		cfg.routes.add(new routeData("PUT", path, route));
		checkStarted();
	}

	public static void host(String h) {
		if (h == null || h.isEmpty()) {
			CURRENT_HOST = null;
		} else {
			CURRENT_HOST = h;
		}
	}

	public static void host(String hostname, String jksPath, String jksPass) {
		CURRENT_HOST = hostname;
		HostConfig cfg = getHostConfig();
		cfg.jksPath = jksPath;
		cfg.jksPass = jksPass;
	}

	public static void port(int p) {
		checkInstance();
		INSTANCE.portNumber = p;
	}

	public static void before(Route filter) {
		checkInstance();
		HostConfig cfg = getHostConfig();
		cfg.beforeFilters.add(filter);
		checkStarted();
	}

	public static void after(Route filter) {
		checkInstance();
		HostConfig cfg = getHostConfig();
		cfg.afterFilters.add(filter);
		checkStarted();
	}

	public static void securePort(int p) {
		checkInstance();
		INSTANCE.securePort = p;
		if (INSTANCE.portNumber == 0) {
			INSTANCE.portNumber = 80;
		}
	}

	static String generateSessionId() {
		String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789~-";
		StringBuilder id = new StringBuilder(20);
		for (int i = 0; i < 20; i++) {
			id.append(chars.charAt(RANDOM.nextInt(chars.length())));
		}
		return id.toString();
	}

	static SessionImpl createNewSession() {
		while (true) {
			String id = generateSessionId();
			SessionImpl s = new SessionImpl(id);
			SessionImpl prev = SESSIONS.putIfAbsent(id, s);
			if (prev == null) {
				return s;
			}
		}
	}

	private static void checkInstance() {
		if (INSTANCE == null) {
			INSTANCE = new Server();
		}
	}

	private static void checkStarted() {
		if (!launch) {
			launch = true;
			Thread t = new Thread((Runnable) INSTANCE);
			t.start();
		}
	}

	private static final class routeData {
		final String method;
		final String pattern;
		final Route handler;

		routeData(String method, String pattern, Route handler) {
			this.method = method;
			this.pattern = pattern;
			this.handler = handler;
		}
	}

	private static Map<String, String> matchParams(String pattern, String urlPath) {
		if (pattern == null || urlPath == null) {
			return null;
		}
		int start = 0;
		int end = pattern.length();

		while (start < end && pattern.charAt(start) == '/') {
			start++;
		}
		while (end > start && pattern.charAt(end - 1) == '/') {
			end--;
		}
		String p = pattern.substring(start, end);

		start = 0;
		end = urlPath.length();

		while (start < end && urlPath.charAt(start) == '/') {
			start++;
		}
		while (end > start && urlPath.charAt(end - 1) == '/') {
			end--;
		}
		String u = urlPath.substring(start, end);

		if (p.isEmpty() && u.isEmpty()) {
			return new LinkedHashMap<>();
		}

		String[] patternParts;
		if (p.isEmpty()) {
			patternParts = new String[0];
		} else {
			patternParts = p.split("/");
		}

		String[] URLParts;
		if (u.isEmpty()) {
			URLParts = new String[0];
		} else {
			URLParts = u.split("/");
		}

		if (patternParts.length != URLParts.length) {
			return null;
		}

		Map<String, String> params = new LinkedHashMap<>();
		for (int i = 0; i < patternParts.length; i++) {
			String patternPart = patternParts[i];
			String URLPart = URLParts[i];

			if (patternPart.startsWith(":")) {
				String name = patternPart.substring(1);
				if (name.isEmpty()) {
					return null;
				}
				params.put(name, URLPart);
			} else {
				if (!patternPart.equals(URLPart)) {
					return null;
				}
			}
		}
		return params;
	}

	private static void parseQueryParams(String query, Map<String, String> target) throws Exception {
		if (query == null || query.isEmpty()) {
			return;
		}
		int start = 0;
		while (start <= query.length()) {
			int ampIdx = query.indexOf('&', start);

			String pair;
			if (ampIdx == -1) {
				pair = query.substring(start);
			} else {
				pair = query.substring(start, ampIdx);
			}

			if (!pair.isEmpty()) {
				int eqIdx = pair.indexOf('=');
				String key, val;
				if (eqIdx >= 0) {
					key = pair.substring(0, eqIdx);
					val = pair.substring(eqIdx + 1);
				} else {
					key = pair;
					val = "";
				}
				String keyUTF8 = URLDecoder.decode(key, StandardCharsets.UTF_8.name());
				String valUTF8 = URLDecoder.decode(val, StandardCharsets.UTF_8.name());
				if (keyUTF8 != null && !keyUTF8.isEmpty()) {
					if (target.containsKey(keyUTF8)) {
						target.put(keyUTF8, target.get(keyUTF8) + "," + valUTF8);
					} else {
						target.put(keyUTF8, valUTF8);
					}
				}
			}
			if (ampIdx == -1) {
				break;
			}
			start = ampIdx + 1;
		}
	}

	private static String hostKey(String host) {
		if (host == null || host.isEmpty()) {
			return DEFAULT_HOST;
		}
		String hostLower = host.toLowerCase();
		int colonIdx = hostLower.indexOf(':');
		if (colonIdx >= 0) {
			return hostLower.substring(0, colonIdx);
		} else {
			return hostLower;
		}
	}

	private static final class HostConfig {
		final CopyOnWriteArrayList<routeData> routes = new CopyOnWriteArrayList<>();
		final CopyOnWriteArrayList<Route> beforeFilters = new CopyOnWriteArrayList<>();
		final CopyOnWriteArrayList<Route> afterFilters = new CopyOnWriteArrayList<>();
		volatile String staticDir = null;
		volatile boolean staticEnabled = false;
		String jksPath;
		String jksPass;
		volatile SSLContext sslContext;
	}

	private static HostConfig getHostConfig() {
		String key = hostKey(CURRENT_HOST);
		return HOSTS.computeIfAbsent(key, k -> new HostConfig());
	}

	private static HostConfig getReqHost(String hostHeader) {
		String reqKey = hostKey(hostHeader);
		HostConfig cfg = HOSTS.get(reqKey);
		if (cfg != null) {
			return cfg;
		}
		return HOSTS.get(DEFAULT_HOST);
	}

	private static SSLContext contextForHost(String hostname) throws Exception {
		HostConfig cfg = HOSTS.get(hostKey(hostname));
		if (cfg == null) {
			cfg = HOSTS.get(DEFAULT_HOST);
		}
		if (cfg.sslContext == null) {
			synchronized (cfg) {
				if (cfg.sslContext == null) {

					String path;
					if (cfg.jksPath != null) {
						path = cfg.jksPath;
					} else {
						path = "keystore.jks";
					}

					String pass;
					if (cfg.jksPass != null) {
						pass = cfg.jksPass;
					} else {
						pass = "secret";
					}
					cfg.sslContext = createSSLContext(path, pass);
				}
			}
		}
		return cfg.sslContext;
	}

	private static SSLContext createSSLContext(String jksPath, String jksPass) throws Exception {
		KeyStore ks = KeyStore.getInstance("JKS");
		try (FileInputStream jksFile = new FileInputStream(jksPath)) {
			ks.load(jksFile, jksPass.toCharArray());
		}
		KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
		kmf.init(ks, jksPass.toCharArray());
		SSLContext sslContext = SSLContext.getInstance("TLS");
		sslContext.init(kmf.getKeyManagers(), null, null);
		return sslContext;
	}

	private void serverLoop(ServerSocket ssock) throws IOException {
		while (true) {
			Socket sock = ssock.accept();
			try {
				sock.setTcpNoDelay(true);
				QUEUE.put(sock);
			} catch (InterruptedException e) {
				sock.close();
				return;
			}
		}
	}

	private static void removeExpiredSession() {
		Thread t = new Thread(() -> {
			try {
				while (true) {
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						return;
					}
					for (Map.Entry<String, SessionImpl> e : SESSIONS.entrySet()) {
						SessionImpl s = e.getValue();
						if (!s.isValid()) {
							SESSIONS.remove(e.getKey(), s);
						}
					}
				}
			} catch (Exception e) {

			}
		}, "removeExpiredSession");

		t.setDaemon(true);
		t.start();
	}
}
