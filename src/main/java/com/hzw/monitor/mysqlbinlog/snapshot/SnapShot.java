package com.hzw.monitor.mysqlbinlog.snapshot;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.alibaba.fastjson.JSONObject;
import com.hzw.monitor.mysqlbinlog.type.ParallelType;
import com.hzw.monitor.mysqlbinlog.utils.WriteResultUtils;

public class SnapShot {
	// 全局valid标志
	private AtomicBoolean globalValid;
	// 大的类型Rotate:IO
	private SnapShotType type;
	// 如果是io类型，是否接收不被过滤掉?
	private boolean accepted;
	// 如果接受，相应的数据
	private String db;
	private String table;
	private ArrayList<JSONObject> datas;
	// 存储结果
	private AtomicInteger writeResult;// 默认失败,这个变量会在多线程情况下被使用
	// 更新到zk
	private String zkValue;
	// 并行类型：表：库
	private ParallelType parallelType;
	private ArrayList<String> pks;
	AtomicReference<SnapShot> next;

	public SnapShot getNext() {
		return next.get();
	}

	public void setNext(SnapShot next) {
		this.next.set(next);
	}

	public boolean isAccepted() {
		return accepted;
	}

	public SnapShot(AtomicBoolean _globalValid, SnapShotType _type, boolean _accepted, String _db, String _table,
			ArrayList<JSONObject> _datas, String _zkValue, ParallelType pType, ArrayList<String> _pks) {
		globalValid = _globalValid;
		type = _type;
		accepted = _accepted;
		db = _db;
		table = _table;
		datas = _datas;
		writeResult = new AtomicInteger(WriteResultUtils.PENDING.ordinal());// 默认未知情况,1表示成功，-1表示失败
		zkValue = _zkValue;
		next = new AtomicReference<SnapShot>(null);
		// 设置并行类型
		parallelType = pType;
		pks = _pks;
	}

	public ArrayList<String> getPks() {
		return pks;
	}

	public ParallelType getParallelType() {
		return parallelType;
	}

	public void setWriteResult(int result) {
		writeResult.set(result);
	}

	public int getWriteResult() {
		return writeResult.get();
	}

	public String getZkValue() {
		return zkValue;
	}

	public SnapShotType getType() {
		return type;
	}

	public String getDb() {
		return db;
	}

	public ArrayList<JSONObject> getDatas() {
		return datas;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public AtomicBoolean getGlobalValid() {
		return globalValid;
	}
}
