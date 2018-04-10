package com.hzw.monitor.mysqlbinlog.event.data;

import java.util.ArrayList;

import com.alibaba.fastjson.JSONObject;
import com.hzw.monitor.mysqlbinlog.event.EventData;
import com.hzw.monitor.mysqlbinlog.event.EventHeader;

public class XidEventData implements EventData {

	private static final long serialVersionUID = 1830588105284956838L;
	
	private long xid;

	public long getXid() {
		return xid;
	}

	public void setXid(long xid) {
		this.xid = xid;
	}

	@Override
	public String toString() {
		return "XidEventData [xid=" + xid + "]";
	}

	@SuppressWarnings("unused")
	private EventHeader header;

	@Override
	public void setEventHeader(EventHeader h) {
		this.header = h;
	}

	@Override
	public ArrayList<JSONObject> toJson() {
		return null;
	}

}
