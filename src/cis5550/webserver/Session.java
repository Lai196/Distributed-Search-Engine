package cis5550.webserver;

public interface Session {

	String id();

	long creationTime();

	long lastAccessedTime();

	int maxActiveInterval();

	void maxActiveInterval(int seconds);

	void invalidate();

	Object attribute(String name);

	void attribute(String name, Object value);
};
