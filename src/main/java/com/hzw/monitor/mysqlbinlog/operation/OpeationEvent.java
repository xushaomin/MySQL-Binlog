package com.hzw.monitor.mysqlbinlog.operation;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

public class OpeationEvent {
	private PathChildrenCacheEvent event;
	private OperationType type;

	public OpeationEvent(PathChildrenCacheEvent e, OperationType t) {
		event = e;
		type = t;
	}

	public OperationType getType() {
		return type;
	}

	public PathChildrenCacheEvent getEvent() {
		return event;
	}

}
