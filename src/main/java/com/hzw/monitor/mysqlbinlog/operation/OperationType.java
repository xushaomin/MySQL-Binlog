package com.hzw.monitor.mysqlbinlog.operation;

public enum OperationType {
	TASK_ADD, TASK_UPDATE, TASK_DELETE, RUNNING_ADD, RUNNING_UPDATE, RUNNING_DELETE,
	//
	SOCKET_ADD, SOCKET_DELETE,
	//
	MACHINE_ADD, MACHINE_UPDATE, MACHINE_DELETE
}
