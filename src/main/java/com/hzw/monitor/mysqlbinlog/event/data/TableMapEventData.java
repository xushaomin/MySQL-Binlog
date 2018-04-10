package com.hzw.monitor.mysqlbinlog.event.data;

import java.util.ArrayList;
/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 *
 */
import java.util.Arrays;
import java.util.BitSet;

import com.alibaba.fastjson.JSONObject;
import com.hzw.monitor.mysqlbinlog.event.EventData;
import com.hzw.monitor.mysqlbinlog.event.EventHeader;

public class TableMapEventData implements EventData {
	/**
	 * @author:liuzhiqiang
	 * @qq: 837500869
	 */
	private static final long serialVersionUID = 7321857335203170267L;
	
	// 最重要的东西，就是列名
	private long tableId;
	private String database;
	private String table;
	private byte[] columnTypes;
	private int[] columnMetadata;
	private BitSet columnNullability;

	public long getTableId() {
		return tableId;
	}

	public void setTableId(long tableId) {
		this.tableId = tableId;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public byte[] getColumnTypes() {
		return columnTypes;
	}

	public void setColumnTypes(byte[] columnTypes) {
		this.columnTypes = columnTypes;
	}

	public int[] getColumnMetadata() {
		return columnMetadata;
	}

	public void setColumnMetadata(int[] columnMetadata) {
		this.columnMetadata = columnMetadata;
	}

	public BitSet getColumnNullability() {
		return columnNullability;
	}

	public void setColumnNullability(BitSet columnNullability) {
		this.columnNullability = columnNullability;
	}

	@Override
	public String toString() {
		return "TableMapEventData [tableId=" + tableId + ", database=" + database + ", table=" + table
				+ ", columnTypes=" + Arrays.toString(columnTypes) + ", columnMetadata="
				+ Arrays.toString(columnMetadata) + ", columnNullability=" + columnNullability + "]";
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
