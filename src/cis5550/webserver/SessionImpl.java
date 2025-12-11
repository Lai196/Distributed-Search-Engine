package cis5550.webserver;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class SessionImpl implements Session {
	private final String id;
	private final long creationTime;
	private volatile long lastAccessedTime;
	private volatile int maxInactiveInterval = 300;
	private final ConcurrentMap<String, Object> attributes = new ConcurrentHashMap<>();
	private volatile boolean invalidated = false;

	SessionImpl(String id) {
		this.id = id;
		long now = System.currentTimeMillis();
		this.creationTime = now;
		this.lastAccessedTime = now;
	}

	@Override
	public String id() {
		return id;
	}

	@Override
	public long creationTime() {
		return creationTime;
	}

	@Override
	public long lastAccessedTime() {
		return lastAccessedTime;
	}

	@Override
	public int maxActiveInterval() {
		return maxInactiveInterval;
	}

	@Override
	public void maxActiveInterval(int seconds) {
		this.maxInactiveInterval = seconds;
	}

	@Override
	public void invalidate() {
		invalidated = true;
		attributes.clear();
	}

	@Override
	public Object attribute(String name) {
		return attributes.get(name);
	}

	@Override
	public void attribute(String name, Object value) {
		if (value == null) {
			attributes.remove(name);
		} else {
			attributes.put(name, value);
		}
	}

	public void access() {
		this.lastAccessedTime = System.currentTimeMillis();
	}

	public boolean isValid() {
		if (invalidated)
			return false;
		if (maxInactiveInterval < 0)
			return true;
		long idle = System.currentTimeMillis() - lastAccessedTime;
		return idle < (maxInactiveInterval * 1000L);
	}
}
