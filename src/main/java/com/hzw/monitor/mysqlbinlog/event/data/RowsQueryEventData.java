package com.hzw.monitor.mysqlbinlog.event.data;

import java.util.ArrayList;

import com.alibaba.fastjson.JSONObject;
import com.hzw.monitor.mysqlbinlog.event.EventData;
import com.hzw.monitor.mysqlbinlog.event.EventHeader;

/**
 * 
 * @author gqliu 2016年1月13日
 *
 */
public class RowsQueryEventData implements EventData {

	private static final long serialVersionUID = 8263641770089808861L;

	private String query;

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("RowsQueryEventData");
		sb.append("{query='").append(query).append('\'');
		sb.append('}');
		return sb.toString();
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
