package com.hzw.monitor.mysqlbinlog.event;
/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 *
 */
public class EventHeader {
	// v1 (MySQL 3.23)
	private long timestamp;
	private EventType eventType;
	private long serverId;
	private long eventLength;
	// v3 (MySQL 4.0.2-4.1)
	private long nextPosition;
	private int flags;

	public EventHeader() {

	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public EventType getEventType() {
		return eventType;
	}

	public void setEventType(EventType eventType) {
		this.eventType = eventType;
	}

	public long getServerId() {
		return serverId;
	}

	public void setServerId(long serverId) {
		this.serverId = serverId;
	}

	public long getEventLength() {
		return eventLength;
	}

	public void setEventLength(long eventLength) {
		this.eventLength = eventLength;
	}

	public long getNextPosition() {
		return nextPosition;
	}

	public void setNextPosition(long nextPosition) {
		this.nextPosition = nextPosition;
	}

	public int getFlag() {
		return flags;
	}

	public long getHeaderLength() {
		return 19;
	}

	public long getDataLength() {
		return eventLength - getHeaderLength();
	}

	@Override
	public String toString() {
		return "EventHeader [timestamp=" + timestamp + ", eventType=" + eventType + ", serverId=" + serverId
				+ ",headerLength=19" + ", dataLength=" + this.getDataLength() + ", nextPosition=" + nextPosition + ", flags="
				+ flags + "]";
	}

	public void setFlag(int flag) {
		this.flags = flag;
	}
}
