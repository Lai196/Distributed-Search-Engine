package cis5550.webserver;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ResponseImpl implements Response {

	private int statusCode = 200;
	private String statusReason = "OK";
	private String contentType = "text/html";
	private final Map<String, List<String>> headers = new LinkedHashMap<>();
	private byte[] body = null;
	private boolean writeStatus = false;
	private OutputStream out;
	private boolean headersSent = false;
	private boolean isHead = false;
	private boolean halted = false;
	private int haltCode;
	private String haltReason;

	@Override
	public void body(String bodyStr) {
		if (writeStatus) {
			return;
		}
		if (bodyStr != null) {
			this.body = bodyStr.getBytes(StandardCharsets.UTF_8);
		} else {
			this.body = null;
		}
	}

	@Override
	public void bodyAsBytes(byte[] bodyArg) {
		if (writeStatus) {
			return;
		}
		this.body = bodyArg;
	}

	@Override
	public void header(String name, String value) {
		if (writeStatus) {
			return;
		}
		List<String> values = headers.get(name);
		if (values == null) {
			values = new ArrayList<>();
			headers.put(name, values);
		}
		values.add(value);
	}

	@Override
	public void type(String contentType) {
		if (writeStatus) {
			return;
		}
		this.contentType = contentType;
	}

	@Override
	public void status(int statusCode, String reasonPhrase) {
		if (writeStatus) {
			return;
		}
		this.statusCode = statusCode;
		this.statusReason = reasonPhrase;
	}

	@Override
	public void write(byte[] b) throws Exception {
		if (!headersSent) {
            String header =
            	    "HTTP/1.1 " + statusCode + " " + statusReason + "\r\n"
            	    + "Server: SecondTry\r\n"
            	    + "Connection: close\r\n"
            	    + "Content-Type: " + this.contentType + "\r\n";

			for (Map.Entry<String, List<String>> k : headers.entrySet()) {
				String headerName = k.getKey();
				for (String headerVal : k.getValue()) {
					header = header + headerName + ": " + headerVal + "\r\n";
				}
			}
			header = header + "\r\n";

			out.write(header.getBytes());
			out.flush();
			headersSent = true;
			writeStatus = true;
		}

		if (!isHead && b != null && b.length > 0) {
			out.write(b);
			out.flush();
		}
	}

	@Override
	public void redirect(String url, int responseCode) {
		if (writeStatus) {
			return;
		}

		int code;
		if (responseCode == 301 || responseCode == 302 || responseCode == 303 || responseCode == 307
				|| responseCode == 308) {
			code = responseCode;
		} else {
			code = 302;
		}

		this.statusCode = code;
		this.statusReason = getCodeReason(code);

        try {
            String header =
                "HTTP/1.1 " + statusCode + " " + statusReason + "\r\n" 
                + "Server: SecondTry\r\n"
                + "Connection: close\r\n"
                + "Location: " + url + "\r\n"
                + "Content-Length: 0\r\n";

			for (Map.Entry<String, List<String>> e : headers.entrySet()) {
				String name = e.getKey();
				for (String headerVal : e.getValue()) {
					header = header + name + ": " + headerVal + "\r\n";
				}
			}
			header = header + "\r\n";

			out.write(header.getBytes());
			out.flush();
			headersSent = true;
			writeStatus = true;
		} catch (Exception ignore) {
		}
	}

	@Override
	public void halt(int statusCode, String reasonPhrase) {
		if (writeStatus) {
			return;
		}
		this.halted = true;
		this.haltCode = statusCode;

		if (reasonPhrase != null) {
			this.haltReason = reasonPhrase;
		} else {
			this.haltReason = "";
		}

		this.statusCode = statusCode;
		if (reasonPhrase != null && !reasonPhrase.isEmpty()) {
			this.statusReason = reasonPhrase;
		} else {
			this.statusReason = getCodeReason(statusCode);
		}

		byte[] b = this.body;

		int bodyLen;
		if (b == null) {
			bodyLen = 0;
		} else {
			bodyLen = b.length;
		}

        try {
            String header =
                "HTTP/1.1 " + this.statusCode + " " + this.statusReason + "\r\n"
                + "Server: SecondTry\r\n"
                + "Connection: close\r\n"
                + "Content-Type: " + this.contentType + "\r\n"
                + "Content-Length: " + bodyLen + "\r\n";

			for (Map.Entry<String, List<String>> e : headers.entrySet()) {
				String name = e.getKey();
				for (String headerVal : e.getValue()) {
					header = header + name + ": " + headerVal + "\r\n";
				}
			}
			header = header + "\r\n";

			out.write(header.getBytes());
			if (!isHead && bodyLen > 0) {
				out.write(b);
			}
			out.flush();
			headersSent = true;
			writeStatus = true;
		} catch (Exception e) {

		}
	}

	private String getCodeReason(int code) {
		if (code == 200) {
			return "OK";
		} else if (code == 301) {
			return "Moved Permanently";
		} else if (code == 302) {
			return "Found";
		} else if (code == 303) {
			return "See Other";
		} else if (code == 307) {
			return "Temporary Redirect";
		} else if (code == 308) {
			return "Permanent Redirect";
		} else if (code == 400) {
			return "Bad Request";
		} else if (code == 403) {
			return "Forbidden";
		} else if (code == 404) {
			return "Not Found";
		} else if (code == 500) {
			return "Internal Server Error";
		}
		return null;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public String getStatusReason() {
		return statusReason;
	}

	public String getContentType() {
		return contentType;
	}

	public Map<String, List<String>> getHeaders() {
		return headers;
	}

	public byte[] getBody() {
		return body;
	}

	public boolean isWriteStatus() {
		return writeStatus;
	}

	public boolean isHalted() {
		return halted;
	}

	public int getHaltCode() {
		return haltCode;
	}

	public String getHaltReason() {
		return haltReason;
	}

	void setOutputStream(OutputStream out) {
		this.out = out;
	}

	void setHead(boolean isHead) {
		this.isHead = isHead;
	}

}
