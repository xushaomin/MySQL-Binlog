package com.hzw.monitor.mysqlbinlog.packet;

/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 *
 */
import java.util.LinkedList;
import java.util.List;

import com.hzw.monitor.mysqlbinlog.utils.ByteUtils;

import io.netty.buffer.ByteBuf;

public class RowPacket {

	private String[] values;

	public RowPacket(ByteBuf msg) {
		// LoggerUtils.debug(logger, "generate RowPacket");
		List<String> tempList = new LinkedList<String>();
		// 读取
		while (msg.isReadable()) {// 还有数据可读
			Number num = ByteUtils.readVariableNumber(msg);
			int length = num.intValue();
			// 获取了长度，读取指定的字节
			String str = ByteUtils.readSpecifiedLengthString(msg, length);
			tempList.add(str);
		}
		values = tempList.toArray(new String[tempList.size()]);
		// LoggerUtils.debug(logger, values.toString());
	}

	public String getValues(int index) {
		if (index >= 0 && index < values.length) {
			return values[index];
		}
		return null;

	}
}
