package com.hzw.monitor.mysqlbinlog.handlers;

import java.util.ArrayList;
/**
 * 
 * @author zhiqiang.liu
 * @2016年1月14日
 * @qq:837500869
 */
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.hzw.monitor.mysqlbinlog.connection.ConnectionAttributes;
import com.hzw.monitor.mysqlbinlog.consumer.ConsumerMananger;
import com.hzw.monitor.mysqlbinlog.event.EventData;
import com.hzw.monitor.mysqlbinlog.event.EventHeader;
import com.hzw.monitor.mysqlbinlog.event.EventType;
import com.hzw.monitor.mysqlbinlog.event.RowEventData;
import com.hzw.monitor.mysqlbinlog.event.data.RotateEventData;
import com.hzw.monitor.mysqlbinlog.netty.server.MyNioSocketChannel;
import com.hzw.monitor.mysqlbinlog.parser.DeleteRowsEventDataParser;
import com.hzw.monitor.mysqlbinlog.parser.EventDataParser;
import com.hzw.monitor.mysqlbinlog.parser.FormatDescriptionParser;
import com.hzw.monitor.mysqlbinlog.parser.NullEventDataParser;
import com.hzw.monitor.mysqlbinlog.parser.QueryEventDataParser;
import com.hzw.monitor.mysqlbinlog.parser.RotateEventDataParser;
import com.hzw.monitor.mysqlbinlog.parser.RowsQueryEventDataParser;
import com.hzw.monitor.mysqlbinlog.parser.TableMapEventDataParser;
import com.hzw.monitor.mysqlbinlog.parser.UpdateRowsEventDataParser;
import com.hzw.monitor.mysqlbinlog.parser.WriteRowsEventDataParser;
import com.hzw.monitor.mysqlbinlog.parser.XidEventDataParser;
import com.hzw.monitor.mysqlbinlog.snapshot.SnapShot;
import com.hzw.monitor.mysqlbinlog.snapshot.SnapShotType;
import com.hzw.monitor.mysqlbinlog.type.ParallelType;
import com.hzw.monitor.mysqlbinlog.utils.ByteUtils;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;
import com.hzw.monitor.mysqlbinlog.utils.MyConstants;
import com.hzw.monitor.mysqlbinlog.utils.NettyUtils;
import com.hzw.monitor.mysqlbinlog.utils.TimeUtils;
import com.hzw.monitor.mysqlbinlog.zookeeper.ZooKeeperUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class BinlogEventParseHandler extends SimpleChannelInboundHandler<ByteBuf> {
	// http://dev.mysql.com/doc/internals/en/binlog-event.html
	private static final Logger logger = LogManager.getLogger(BinlogEventParseHandler.class);
	private static final EventType[] EVENT_TYPES = EventType.values();
	//private boolean enterLargeBlockPacket = false;// 暂时不支持这种情况,TODO
	private HashMap<EventType, EventDataParser> parsers = new HashMap<EventType, EventDataParser>();
	// private long totalIOEvent = 0;

	public BinlogEventParseHandler() {
		// null_event
		parsers.put(EventType.UNKNOWN, new NullEventDataParser());
		// rotate
		parsers.put(EventType.ROTATE, new RotateEventDataParser());
		// format_description
		parsers.put(EventType.FORMAT_DESCRIPTION, new FormatDescriptionParser());
		// query
		parsers.put(EventType.QUERY, new QueryEventDataParser());
		// table_map
		parsers.put(EventType.TABLE_MAP, new TableMapEventDataParser());
		// delete_rows
		parsers.put(EventType.DELETE_ROWS, new DeleteRowsEventDataParser(false));
		// ext_delete_rows
		parsers.put(EventType.EXT_DELETE_ROWS, new DeleteRowsEventDataParser(true));
		// write_rows
		parsers.put(EventType.WRITE_ROWS, new WriteRowsEventDataParser(false));
		// ext_write_rows
		parsers.put(EventType.EXT_WRITE_ROWS, new WriteRowsEventDataParser(true));
		// update_rows
		parsers.put(EventType.UPDATE_ROWS, new UpdateRowsEventDataParser(false));
		// ext_update_rows
		parsers.put(EventType.EXT_UPDATE_ROWS, new UpdateRowsEventDataParser(true));
		// xid
		parsers.put(EventType.XID, new XidEventDataParser());
		// rows_query
		parsers.put(EventType.ROWS_QUERY, new RowsQueryEventDataParser());
	}

	private void handleRotate(EventType eventType, EventHeader header, EventData eventData,
			ConnectionAttributes myAttributes, ChannelHandlerContext context) throws Exception {
		// newValue
		RotateEventData rotateData = (RotateEventData) eventData;
		String newBinlogFileName = rotateData.getBinlogFilename();
		long newBinlogPosition = rotateData.getBinlogPosition();
		String newValue = newBinlogFileName + ":" + newBinlogPosition;
		// localValue
		String localValue = myAttributes.getBinlogFileName() + ":" + myAttributes.getBinlogPosition();
		// 条件更新
		if (false == newValue.equals(localValue)) {
			// 必须及时修改，别忘记了外面还有个historyZK路径需要修改的，所以要及时更新
			myAttributes.updateBinlogNameAndPosition(newBinlogFileName, newBinlogPosition);
			// 修改为放入taskThread线程，而不是立即更新到ZK,因为更新ZK很耗费时间
			// 挂载到队尾
			// 因为是AtomicReference，所以保证了线程安全性
			SnapShot snapShot = new SnapShot(myAttributes.getGolbalValid(), SnapShotType.ROTATE, false, null, null,
					null, newValue + ":" + System.currentTimeMillis(), null, null);
			myAttributes.addTaskToTail(snapShot);
			LoggerUtils.debug(logger,
					"receive rotate event,not same as local,[BinlogEventParseHandler] send RequestBinaryLogCommand: "
							+ newBinlogFileName + ":" + newBinlogPosition);
			// 测试发现也可以不发送此命令，就取消了，减少线程互斥带来的问题
			// new RequestBinaryLogCommand(myAttributes.getClientId(),
			// newBinlogFileName, newBinlogPosition)
			// .write(context);
		} else {
			// 直接忽略,主要是启动时有一个默认的rotate事件
			LoggerUtils.debug(logger, "recive rotate event,the same as local data,omit");
		}

	}

	private void handleIO(EventType eventType, EventHeader header, EventData eventData,
			ConnectionAttributes myAttributes, ChannelHandlerContext context) throws Exception {
		LoggerUtils.debug(logger, eventType.toString());
		// 1)存储成功了就更新内存变量
		myAttributes.updateBinlogNameAndPosition(myAttributes.getBinlogFileName(), header.getNextPosition());
		// 2)存储快照
		if (header.getNextPosition() > 0) {
			RowEventData rowEventData = (RowEventData) eventData;
			String database = rowEventData.getDatabase();
			String table = rowEventData.getTable();
			boolean accepted = myAttributes.acceptByFilter(database, table);
			ArrayList<JSONObject> datas = accepted ? eventData.toJson() : new ArrayList<JSONObject>();
			String zkValues = myAttributes.getBinlogFileName() + ":" + myAttributes.getBinlogPosition() + ":"
					+ System.currentTimeMillis();
			SnapShot snapShot;
			ArrayList<JSONObject> tmpList;
			// 准备工作完毕
			if (false == accepted) {// 需要丢弃的，accepted保证不处理,consumer是不会处理的
				snapShot = new SnapShot(myAttributes.getGolbalValid(), SnapShotType.IO, accepted, database, table,
						datas, zkValues, ParallelType.TABLE, null);
				ConsumerMananger.getInstance().addSnapShot(snapShot, myAttributes);
				myAttributes.addTaskToTail(snapShot);
			} else {// 确实需要处理的
				// 根据是否有主键来决定按表加速还是按行加速
				ArrayList<String> pks = myAttributes.getPrimaryKeys(database, table);

				// 是否接受
				if (null == pks || pks.isEmpty()) {// 按表复制-退化
					snapShot = new SnapShot(myAttributes.getGolbalValid(), SnapShotType.IO, accepted, database, table,
							datas, zkValues, ParallelType.TABLE, null);
					ConsumerMananger.getInstance().addSnapShot(snapShot, myAttributes);
					myAttributes.addTaskToTail(snapShot);
				} else {// 按行复制-加速-分拆，最后一条带上ZK信息，其它不带
					int index = 0;
					int size = datas.size();
					for (JSONObject data : datas) {
						tmpList = new ArrayList<JSONObject>();
						tmpList.add(data);
						index++;
						if (index != size) {
							snapShot = new SnapShot(myAttributes.getGolbalValid(), SnapShotType.IO, accepted, database,
									table, tmpList, null, ParallelType.ROW, pks);
						} else {// 最后一条，带上zk
							snapShot = new SnapShot(myAttributes.getGolbalValid(), SnapShotType.IO, accepted, database,
									table, tmpList, zkValues, ParallelType.ROW, pks);
						}
						ConsumerMananger.getInstance().addSnapShot(snapShot, myAttributes);
						myAttributes.addTaskToTail(snapShot);
					}
				}
			}
			// LoggerUtils.debug(logger, "insert into task thread queue
			// succeed");
		} else {
			LoggerUtils.error(logger, "nextBinlogPosition:" + header.getNextPosition());
			throw new Exception("io data,but nextBinlogPosition is 0");
		}

	}

	private void handlerDefault(EventType eventType, EventHeader header, EventData eventData,
			ConnectionAttributes myAttributes, ChannelHandlerContext context) {
		// 其它不处理的事情都在这里
		// 这里其实不能写业务[比如更新本地内存位置]，否则下次获取会出错
	}

	private void handleXID(EventType eventType, EventHeader header, EventData eventData,
			ConnectionAttributes myAttributes, ChannelHandlerContext context) {
		// 直接加到尾巴上，作为一个完整事件的结束
		SnapShot snapShot = new SnapShot(myAttributes.getGolbalValid(), SnapShotType.XID, false, null, null, null, null,
				null, null);
		myAttributes.addTaskToTail(snapShot);
	}

	private void handle(EventType eventType, EventHeader header, EventData eventData, ConnectionAttributes myAttributes,
			ChannelHandlerContext context) throws Exception {
		LoggerUtils.debug(logger, "enter handle");
		// 保证参数OK
		if (null == eventType || null == eventData || null == myAttributes) {
			return;
		}
		switch (eventType) {
		case ROTATE:
			handleRotate(eventType, header, eventData, myAttributes, context);
			break;
		case WRITE_ROWS:
		case EXT_WRITE_ROWS:
		case UPDATE_ROWS:
		case EXT_UPDATE_ROWS:
		case DELETE_ROWS:
		case EXT_DELETE_ROWS:
			handleIO(eventType, header, eventData, myAttributes, context);
			break;
		case XID:
			handleXID(eventType, header, eventData, myAttributes, context);
			break;
		default:// 其它事件如何处理
			handlerDefault(eventType, header, eventData, myAttributes, context);
			break;
		}
		LoggerUtils.debug(logger, "leave handle");
	}

	private void handleZKHistoryRecord(EventHeader header, ConnectionAttributes myAttributes) {
		// 每一条都会处理,不区分具体事件类型
		// LoggerUtils.debug(logger, "enter handleZKHistoryRecord");
		long t = header.getTimestamp();
		LoggerUtils.debug(logger, "timestamp:" + t);
		if (header.getEventType() == EventType.ROTATE && t == 0) {
			// 这种情况是需要过滤的
			return;
		}
		// 如果是第一条formatDescription忽略
		// 后面的formatDescription暂时没发现问题
		if (myAttributes.isFirstFormatDescription()) {
			myAttributes.setFirstFormatDescription(false);
			return;
		}
		// 修复query|ROWS_QUERY中时间戳为0的情况
		if (EventType.QUERY == header.getEventType() || EventType.ROWS_QUERY == header.getEventType()) {
			if (0 == t) {
				return;// 直接忽略
			}
		}
		// 可以处理了
		try {
			String day = TimeUtils.getTimeDay(t);
			LoggerUtils.debug(logger, "type:" + header.getEventType());
			LoggerUtils.debug(logger, "timestamp:" + t + " day:" + day);
			myAttributes.setCurrentPosition(myAttributes.getNextPosition());
			myAttributes.setNextPosition(header.getNextPosition());
			// 第一次进来先记下当前天
			if (myAttributes.getHistoryPositionDay() == null) {
				myAttributes.setHistoryPositionDay(day);
			} else if (!day.equals(myAttributes.getHistoryPositionDay())) {
				// 需要记录到zk
				LoggerUtils.info(logger, "write history record: " + day + " --- " + myAttributes.getBinlogFileName()
						+ ":" + myAttributes.getCurrentPosition());
				String historyPath = myAttributes.getBinlogPositionZKPath() + "/" + day;
				if (!ZooKeeperUtils.exist(historyPath)) {
					ZooKeeperUtils.createPersistent(historyPath, myAttributes.getBinlogFileName() + ":"
							+ myAttributes.getCurrentPosition() + ":" + System.currentTimeMillis());
				} else {
					ZooKeeperUtils.update(historyPath, myAttributes.getBinlogFileName() + ":"
							+ myAttributes.getCurrentPosition() + ":" + System.currentTimeMillis());
				}
				LoggerUtils.debug(logger, "write record finish...");
				myAttributes.setHistoryPositionDay(day);
				// 并判断zk是否保存了30天的数据，如果超过则删除
				List<String> children = ZooKeeperUtils.getChildren(myAttributes.getBinlogPositionZKPath());
				for (String child : children) {
					if (((t - TimeUtils.getDayTime(child))
							/ MyConstants.MILLISECOND_OF_ONE_DAY) > MyConstants.HISTORY_RECORD_DAY) {
						ZooKeeperUtils.deletePath(myAttributes.getBinlogPositionZKPath() + "/" + child);
					}
				}
			}
		} catch (Exception e) {
			LoggerUtils.error(logger, e.toString());
		}
		LoggerUtils.debug(logger, "leave handleZKHistoryRecord");
		// LoggerUtils.debug(logger, "");
	}

	@Override
	protected void channelRead0(ChannelHandlerContext context, ByteBuf msg) throws Exception {
		// 强调的一点是：到这个类之后，才会跟之前初始化的任务线程发生关系，否则有问题
		LoggerUtils.debug(logger, "------------------------------------------");
		LoggerUtils.debug(logger, "BinlogEventParseHandler - channelRead0 enter...");
		// LoggerUtils.info(logger, msg.toString());
		// 如何来做并行加速，就是把数据相关的东西提前取出来，抛到消费线程组里
		try {
			EventHeader header = new EventHeader();
			header.setTimestamp(ByteUtils.readUnsignedLong(msg, 4) * 1000L);
			header.setEventType(EVENT_TYPES[ByteUtils.readUnsignedInt(msg, 1)]);
			header.setServerId(ByteUtils.readUnsignedLong(msg, 4));
			header.setEventLength(ByteUtils.readUnsignedLong(msg, 4));
			header.setNextPosition(ByteUtils.readUnsignedLong(msg, 4));
			header.setFlag(ByteUtils.readUnsignedInt(msg, 2));
			LoggerUtils.debug(logger, header.toString());
			// 2)处理
			// 决定是否更新binlog[name:position]的历史记录
			ConnectionAttributes myAttributes = ((MyNioSocketChannel) context.channel()).getAttributes();
			// 一天才会发生一次，所以留在这里不会影响太大
			handleZKHistoryRecord(header, myAttributes);
			LoggerUtils.debug(logger, "(((type: " + header.getEventType());
			// --------------------
			// 3)获取EventParser
			EventDataParser parser = parsers.get(header.getEventType());
			if (null == parser) {
				// 找不到的事件就不要处理了
				// 但是最好打个日志
				LoggerUtils.info(logger, "parser not found..." + header.getEventType());
			} else {
				int checksumLength = (int) myAttributes.getChecksumLength();
				EventData eventData = parser.parse(msg, context, checksumLength);
				// LoggerUtils.debug(logger, eventData.toString());
				// 4)关联
				eventData.setEventHeader(header);
				// 5)处理各种事件
				handle(header.getEventType(), header, eventData, myAttributes, context);
			}

		} catch (Exception e) {
			LoggerUtils.error(logger, e.toString());
			throw new Exception(e);
		}
		LoggerUtils.debug(logger, "BinlogEventParseHandler - channelRead0 finished...");
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		// Close the connection when an exception is raised.
		// cause.printStackTrace();//务必要关闭
		LoggerUtils.error(logger, cause.toString());
		NettyUtils.cleanChannelContext(ctx, cause);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		LoggerUtils.debug(logger, "[channelInactive] socket is closed by remote server");
		NettyUtils.cleanChannelContext(ctx, null);
	}

}
