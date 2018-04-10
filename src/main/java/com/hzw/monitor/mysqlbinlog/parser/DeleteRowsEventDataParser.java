package com.hzw.monitor.mysqlbinlog.parser;

/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 *
 */
import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.connection.ConnectionAttributes;
import com.hzw.monitor.mysqlbinlog.event.EventData;
import com.hzw.monitor.mysqlbinlog.event.data.DeleteRowsEventData;
import com.hzw.monitor.mysqlbinlog.event.data.TableMapEventData;
import com.hzw.monitor.mysqlbinlog.netty.server.MyNioSocketChannel;
import com.hzw.monitor.mysqlbinlog.utils.ByteUtils;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class DeleteRowsEventDataParser implements EventDataParser {
	private static final Logger logger = LogManager.getLogger(DeleteRowsEventDataParser.class);

	private boolean mayContainExtraInformation = false;

	public DeleteRowsEventDataParser(boolean b) {
		mayContainExtraInformation = b;
	}

	@Override
	public EventData parse(ByteBuf msg, ChannelHandlerContext context, int checksumLength) {
		// LoggerUtils.debug(logger, "");
		long tableId = ByteUtils.readUnsignedLong(msg, 6);
		msg.skipBytes(2);//// reserved
		// 分支
		if (mayContainExtraInformation) {
			int extraInfoLength = ByteUtils.readUnsignedInt(msg, 2); // inputStream.readInteger(2);
			msg.skipBytes(extraInfoLength - 2);
		}
		// 继续处理
		int numberOfColumns = ByteUtils.readVariableNumber(msg).intValue();
		BitSet includedColumns = null;
		try {
			includedColumns = ByteUtils.readBitSet(msg, numberOfColumns, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// 继续处理
		List<Serializable[]> rows = new LinkedList<Serializable[]>();
		ConnectionAttributes myAttributes = ((MyNioSocketChannel) context.channel()).getAttributes();
		TableMapEventData tableMapEventData = myAttributes.getTableMapEventData(tableId);
		if (null == tableMapEventData) {
			LoggerUtils.error(logger, "error,fail to find tableMapEventData ,tableId is:" + tableId);
		}
		String database = tableMapEventData.getDatabase();
		String table = tableMapEventData.getTable();
		try {
			while (msg.readableBytes() > checksumLength) {// 表明还有内容可取
				rows.add(ByteUtils.deserializeRow(tableMapEventData, includedColumns, msg));
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// 返回最终数据
		DeleteRowsEventData eventData = new DeleteRowsEventData();
		eventData.setTableMapEventData(tableMapEventData);
		eventData.setTableId(tableId);
		if (myAttributes.acceptByFilter(database, table)) {
			eventData.setIncludedColumns(includedColumns, myAttributes.getColumnsMapping(database, table));
		}
		eventData.setRows(rows);
		return eventData;
	}

}
