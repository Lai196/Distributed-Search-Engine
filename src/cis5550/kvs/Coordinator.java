package cis5550.kvs;

import static cis5550.webserver.Server.*;

import java.util.Iterator;

import cis5550.tools.HTTP;

public class Coordinator extends cis5550.generic.Coordinator {

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Error: Reruired argument <port>.");
			System.exit(1);
		}
		final int portNum = Integer.parseInt(args[0]);
		port(portNum);
		Coordinator self = new Coordinator();

		self.coordinatorHost = "localhost";
		self.coordinatorPort = portNum;
		self.registerRoutes();

		// get request to return coordinator page
	    get("/", (req, res) -> {
	        res.type("text/html");
	        return """
	            <!doctype html>
	            <html>
	              <head>
	                <title>Coordinator</title>
	              </head>
	              <body>
	                <h1><strong>Coordinator</strong></h1>

	                <h2>Worker List</h2>
	            """ + self.workerTable() + """
	                <h2>Table List</h2>
	            """ + self.tableList() + """
	              </body>
	            </html>
	            """;
	      });

	// get request to view table
	get("/view/:table", (req, res) -> {
		String tableName = req.params("table");
		res.type("text/html");

		// parse page parameter (e.g. ?page=2)
		int page = 0;
		String p = req.queryParams("page");
		if (p != null) {
			try {
				page = Integer.parseInt(p);
			} catch (NumberFormatException e) {
			}
			if (page < 0) {
				page = 0;
			}
		}

		int pageSize = 10;
		int startIndex = page * pageSize;
		int endIndex = startIndex + pageSize;

		String html = 
				"<!doctype html>"
				+ "<html>"
					+ "<head>"
						+ "<title>View " + tableName + "</title>" 
					+ "</head>"
					+ "<body>" 
						+ "<h1>Table: " + tableName + "</h1>"
						+ "<table border=\"1\">"
						+ "<tr>"
							+ "<th>Row Key</th>"
							+ "<th>Columns</th>"
						+ "</tr>";

		try {
			KVSClient client = new KVSClient(self.coordinatorHost + ":" + self.coordinatorPort);
			
			// get total count for pagination
			int totalRows = client.count(tableName);
			
			Iterator<Row> kvsRow = client.scan(tableName);
			int rowIndex = 0;
			int displayedCount = 0;
			
			// Skip to the start of current page
			while (kvsRow.hasNext() && rowIndex < startIndex) {
				kvsRow.next();
				rowIndex++;
			}
			
			// display rows for current page
			while (kvsRow.hasNext() && displayedCount < pageSize) {
				Row r = kvsRow.next();
				html += "<tr>";
				html += "<td>" + r.key() + "</td>";
				html += "<td>";
				for (String col : r.columns()) {
					String v = r.get(col);
					if (v == null) {
						v = "";
					}
					html += "<div>" + col + "= <span>" + escapeHtml(v) + "</span></div>";
				}
				html += "</td></tr>";
				displayedCount++;
			}
			
			if (totalRows == 0) {
				html += "<tr><td colspan=\"2\"><em>No rows found</em></td></tr>";
			}
			
			html += "</table>";
			
			// Pagination controls
			html += "<p>";
			if (page > 0) {
				html += "<a href=\"/view/" + tableName + "?page=" + (page - 1) + "\">Previous</a> ";
			}
			int totalPages = (totalRows + pageSize - 1) / pageSize;
			if (totalPages == 0) totalPages = 1;
			html += "Page " + (page + 1) + " of " + totalPages;
			if (endIndex < totalRows) {
				html += " <a href=\"/view/" + tableName + "?page=" + (page + 1) + "\">Next</a>";
			}
			html += "</p>";
			
		} catch (Exception e) {
			html += "</table>";
			html += "<p>Error: " + e.getMessage() + "</p>";
		}

		html += "<p><a href=\"/\">Back</a></p>";

		html += "</body></html>";
		return html;
	});

		// get request to count table
		get("/count", (req, res) -> {
			String tableName = req.queryParams("table");
			if (tableName == null || tableName.isEmpty()) {
				res.status(400, "Bad Request");
				return "Missing 'table' query parameter";
			}

			try {
				KVSClient client = new KVSClient(self.coordinatorHost + ":" + self.coordinatorPort);
				int count = client.count(tableName);
				res.type("text/plain");
				return String.valueOf(count);
			} catch (Exception e) {
				res.status(500, "Internal Server Error");
				return "Error: " + e.getMessage();
			}
		});

	}

	// register routes
	@Override
	public void registerRoutes() {
		super.registerRoutes();
	}

	// get request to return table list
	private String tableList() {
		String html = "<table border=\"1\"><tr><th style=\"text-align:left\">Tables</th></tr>";

		try {
			String[] live = getWorkers();
			if (live.length == 0) {
				html += "<tr><td>No active workers</td></tr>";
			} else {
				String url = "http://" + live[0] + "/tables";
				byte[] body = HTTP.doRequest("GET", url, null).body();
				String[] tables = new String(body).split("\\r?\\n");

				for (String t : tables) {
					if (!t.isBlank()) {
						html += "<tr><td><a href=\"/view/" + t + "\">" + t + "</a></td></tr>";
					}
				}
			}
		} catch (Exception e) {
			html += "<tr><td>Error fetching tables: " + e.getMessage() + "</td></tr>";
		}

		html += "</table>";
		return html;
	}
	/**
	 * a helper method to escape HTML links in table to prevent rending
	 * @param text
	 * @return escaped replacement text
	 * @author Evan Law
	 */
	private static String escapeHtml(String text) {
		if (text == null) return "";
		return text.replace("&", "&amp;")
				   .replace("<", "&lt;")
				   .replace(">", "&gt;")
				   .replace("\"", "&quot;")
				   .replace("'", "&#x27;");
	}
}
