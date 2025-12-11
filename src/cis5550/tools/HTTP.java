package cis5550.tools;

import java.util.*;
import java.net.*;
import java.io.*;
import javax.net.ssl.*;
import java.security.*;
import java.security.cert.X509Certificate;

public class HTTP {
	public static class Response {
		byte body[];
		Map<String, String> headers;
		int statusCode;

		public Response(byte bodyArg[], Map<String, String> headersArg, int statusCodeArg) {
			body = bodyArg;
			headers = headersArg;
			statusCode = statusCodeArg;
		}

		public byte[] body() {
			return body;
		}

		public int statusCode() {
			return statusCode;
		}

		public Map<String, String> headers() {
			return headers;
		}
	}

	static Socket openSocket(String protocol, String host, int port) {
		try {
			Socket sock = null;
			if (protocol.equals("https")) {
				TrustManager[] trustAllCerts = { new X509TrustManager() {
					public X509Certificate[] getAcceptedIssuers() {
						return null;
					}

					public void checkClientTrusted(X509Certificate[] certs, String authType) {
					}

					public void checkServerTrusted(X509Certificate[] certs, String authType) {
					}
				} };
				SSLContext sc;
				try {
					sc = SSLContext.getInstance("SSL");
					sc.init(null, trustAllCerts, new SecureRandom());
					sock = sc.getSocketFactory().createSocket(host, port);
				} catch (NoSuchAlgorithmException nsae) {
				} catch (KeyManagementException kme) {
				}
			} else if (protocol.equals("http")) {
				sock = new Socket(host, port);
			}

			// set SO_LINGER to skip TIME_WAIT state on Windows
			// to prevents port exhaustion
			if (sock != null) {
				try {
					sock.setSoLinger(true, 0);
				} catch (Exception e) {
				}
			}
			return sock;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	public static Response doRequest(String method, String urlArg, byte uploadOrNull[]) throws IOException {
		return doRequestWithTimeout(method, urlArg, uploadOrNull, -1, false);
	}

	public static Response doRequestWithTimeout(String method, String urlArg, byte uploadOrNull[], int timeoutMillis,
			boolean isHeadRequest) throws IOException {

		boolean triedFresh = false;
		while (true) {
			try {
				String protocol = "http";
				int pos = urlArg.indexOf("://");
				if (pos >= 0) {
					protocol = urlArg.substring(0, pos);
					urlArg = urlArg.substring(pos + 3);
				}
				pos = urlArg.indexOf('/');
				if (pos < 0)
					return null;

				String host = urlArg.substring(0, pos), path = urlArg.substring(pos);
				int port = (protocol.equals("https")) ? 443 : 80;
				pos = host.indexOf(":");
				if (pos > 0) {
					String sport = host.substring(pos + 1);
					try {
						port = Integer.valueOf(sport).intValue();
					} catch (NumberFormatException nfe) {
					}
					host = host.substring(0, pos);
				}

				while (true) {
					Socket sock = openSocket(protocol, host, port);
					if (sock == null)
						throw new IOException("Cannot connect to server " + host + ":" + port);
					sock.setTcpNoDelay(true);

					try {
						if (timeoutMillis > 0)
							sock.setSoTimeout(timeoutMillis);

						OutputStream out = sock.getOutputStream();
						String request = method + " " + path + " HTTP/1.1\r\nHost: " + host + "\r\n";

						if (uploadOrNull != null)
							request = request + "Content-Length: " + uploadOrNull.length + "\r\n";
						request = request + "User-agent: cis5550-crawler\r\nConnection: close\r\n\r\n";
						out.write(request.getBytes());
						if (uploadOrNull != null)
							out.write(uploadOrNull);
						out.flush();
					} catch (IOException ioe) {
						try {
							sock.close();
						} catch (Exception e) {
						}
						throw new IOException(
								"Connection to " + host + ":" + port + " failed while writing the request");
					}

					ByteArrayOutputStream buffer = new ByteArrayOutputStream();
					boolean readingHeaders = true;
					int contentLength = -1;
					Map<String, String> headers = new HashMap<String, String>();
					byte buf[] = new byte[100000];
					int inBuf = 0;
					int statusCode = -1;

					try {
						InputStream in = sock.getInputStream();
						while (true) {
							int n = in.read(buf, inBuf, buf.length - inBuf);
							if (n < 0)
								break;

							inBuf += n;
							if (readingHeaders) {
								int matchPtr = 0;
								for (int i = 0; i < inBuf; i++) {
									if (buf[i] == 10)
										matchPtr++;
									else if (buf[i] != 13)
										matchPtr = 0;

									if (matchPtr == 2) {
										buffer.write(buf, 0, i);
										ByteArrayInputStream bais = new ByteArrayInputStream(buffer.toByteArray());
										BufferedReader hdr = new BufferedReader(new InputStreamReader(bais));
										String statusLine[] = hdr.readLine().split(" ");
										statusCode = Integer.valueOf(statusLine[1]);

										while (true) {
											String s = hdr.readLine();
											if (s.equals(""))
												break;
											String[] p2 = s.split(":", 2);
											if (p2.length == 2) {
												String headerName = p2[0];
												headers.put(headerName.toLowerCase(), p2[1].trim());
												if (headerName.toLowerCase().equals("content-length"))
													contentLength = Integer.parseInt(p2[1].trim());
											}
										}
										buffer.reset();
										System.arraycopy(buf, i + 1, buf, 0, inBuf - (i + 1));
										inBuf -= (i + 1);
										readingHeaders = false;
										break;
									}
								}
							}

							if (!readingHeaders) {
								int toCopy = ((contentLength >= 0) && (inBuf > contentLength)) ? contentLength : inBuf;
								buffer.write(buf, 0, toCopy);
								System.arraycopy(buf, toCopy, buf, 0, inBuf - toCopy);
								inBuf -= toCopy;

								if (isHeadRequest)
									break;
								if ((contentLength >= 0) && (buffer.size() >= contentLength))
									break;
							}
						}
					} catch (Exception e) {
						try {
							sock.close();
						} catch (Exception ei) {
						}
						throw new IOException(
								"Connection to " + host + ":" + port + " failed reading the response (" + e + ")");
					}
					try {
						sock.close();
					} catch (Exception e) {
					}

					return new Response(buffer.toByteArray(), headers, statusCode);
				}
			} catch (IOException io) {
				if (!triedFresh) {
					triedFresh = true;
					continue;
				}
				throw io;
			}
		}

	}
}