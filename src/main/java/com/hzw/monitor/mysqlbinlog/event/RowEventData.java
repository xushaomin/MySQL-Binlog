package com.hzw.monitor.mysqlbinlog.event;

import com.hzw.monitor.mysqlbinlog.event.data.TableMapEventData;

public interface RowEventData extends EventData {
	public void setTableMapEventData(TableMapEventData tableMapEventData);

	public String getDatabase();

	public String getTable();

}
