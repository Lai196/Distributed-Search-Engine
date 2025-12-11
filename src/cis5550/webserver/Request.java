package cis5550.webserver;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Request {

	String ip();

	int port();

	String requestMethod();

	String url();

	String protocol();

	Set<String> headers();

	String headers(String name);

	String contentType();

	String body();

	byte[] bodyAsBytes();

	int contentLength();

	Set<String> queryParams();

	String queryParams(String param);

	Map<String, String> params();

	String params(String name);

	Session session();
};
