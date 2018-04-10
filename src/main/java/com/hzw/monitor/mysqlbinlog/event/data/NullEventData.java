package com.hzw.monitor.mysqlbinlog.event.data;

import java.util.ArrayList;

import com.alibaba.fastjson.JSONObject;
import com.hzw.monitor.mysqlbinlog.event.EventData;
import com.hzw.monitor.mysqlbinlog.event.EventHeader;

public class NullEventData implements EventData {

	private static final long serialVersionUID = 7022857492822395831L;

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
