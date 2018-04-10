package com.hzw.monitor.mysqlbinlog.event.data;

import java.util.ArrayList;

import com.alibaba.fastjson.JSONObject;
import com.hzw.monitor.mysqlbinlog.event.EventData;
import com.hzw.monitor.mysqlbinlog.event.EventHeader;

public class FormatDescriptionEventData implements EventData {

	private static final long serialVersionUID = 1465779438441715909L;

	private int binlogVersion;
	private String serverVersion;
	private int headerLength;

	public int getBinlogVersion() {
		return binlogVersion;
	}

	@Override
	public String toString() {
		return "FormatDescriptionEventData [binlogVersion=" + binlogVersion + ", serverVersion=" + serverVersion
				+ ", headerLength=" + headerLength + "]";
	}

	public void setBinlogVersion(int binlogVersion) {
		this.binlogVersion = binlogVersion;
	}

	public String getServerVersion() {
		return serverVersion;
	}

	public void setServerVersion(String serverVersion) {
		this.serverVersion = serverVersion;
	}

	public int getHeaderLength() {
		return headerLength;
	}

	public void setHeaderLength(int headerLength) {
		this.headerLength = headerLength;
	}

	@SuppressWarnings("unused")
	private EventHeader header;

	@Override
	public void setEventHeader(EventHeader h) {
		this.header = h;
	}

	@Override
	public ArrayList<JSONObject> toJson() {
		// TODO Auto-generated method stub
		return null;
	}

}
