package com.hzw.monitor.mysqlbinlog.operation;

import java.util.ArrayList;
/**
 * 
 * @author zhiqiang.liu
 * @2016年2月26日
 * @Email: 837500869@qq.com
 */
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hzw.monitor.mysqlbinlog.connection.Connection;
import com.hzw.monitor.mysqlbinlog.connection.ConnectionFactory;
import com.hzw.monitor.mysqlbinlog.netty.server.MyNioSocketChannel;
import com.hzw.monitor.mysqlbinlog.netty.server.NettyQueue;
import com.hzw.monitor.mysqlbinlog.utils.IpUtils;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;
import com.hzw.monitor.mysqlbinlog.utils.MetaUtils;
import com.hzw.monitor.mysqlbinlog.utils.MyConstants;
import com.hzw.monitor.mysqlbinlog.utils.NettyUtils;
import com.hzw.monitor.mysqlbinlog.utils.StringUtils;
import com.hzw.monitor.mysqlbinlog.utils.TimeUtils;
import com.hzw.monitor.mysqlbinlog.zookeeper.CuratorFrameworkClient;
import com.hzw.monitor.mysqlbinlog.zookeeper.RunningListener;
import com.hzw.monitor.mysqlbinlog.zookeeper.ZooKeeperUtils;

public class OperationServer {
	
	private static final Logger logger = LogManager.getLogger(OperationServer.class);

	// 很多事件都可以转化为此事件
	public static void AddTask(String taskPath, String taskData, boolean reRegister, PriorityQueue<Machine> queue) {
		// 必须保证，一旦进入这个函数，都是需要进行任务竞争的，否则不应该进入此函数
		LoggerUtils.error(logger, "--------------------------------------------");
		String runningPath = taskPath + "/running";
		String currentPath = runningPath + "/current";
		String binlogPositionPath = taskPath + "/binlog-positions";
		LoggerUtils.error(logger, "--------------------------------------------" + runningPath);

		// 1)保证某个节点必须存在&注册监听器
		ZooKeeperUtils.ensurePersistentPathWithNoValue(runningPath);
		if (reRegister) {
			ZooKeeperUtils.registerChildListener(runningPath, RunningListener.getInstance().getListener());
		}
		// 2)试图抢占tasks/{task}/running临时节点
		{
			// 优化一下，决定是否及时抢占
			if (null != queue) {
				Machine leastSocketMachine = queue.peek();
				if (null != leastSocketMachine) {
					String leastServerId = leastSocketMachine.getServerId();
					if (null != leastServerId && leastServerId.length() > 0) {
						if (false == leastServerId.equals(IpUtils.getServerId())) {
							// 如果不是自己
							// 就应该暂时停一会，给那个人一点时间尽量去抢占
							// 当然这里不能返回，因为那个人可能中间会宕机
							TimeUtils.sleepSeconds(1);
						}
					}
				}
			}
		}
		// 继续抢占
		boolean isMaster = false;
		try {
			CuratorFrameworkClient.getInstance().getClient().create().withMode(CreateMode.EPHEMERAL)
					.forPath(currentPath, IpUtils.getServerId().getBytes());
			isMaster = true;
		} catch (Exception e) {
			isMaster = false;
		}

		// 3)抢占失败的人只好等下次机会了
		if (false == isMaster) {
			return;
		}
		// 4)抢占成功的人有责任保证更新当前[name:position],因为后面[自己|别人]要用
		String fn = "";
		long fp = 0;
		// 得判断是否强制更新
		LoggerUtils.info(logger, "---taskData:" + taskData);
		JSONObject taskJsonObject = JSON.parseObject(taskData);
		String state = taskJsonObject.getString(MyConstants.STATE);
		long taskTimeStamp = taskJsonObject.getLongValue(MyConstants.TIMESTAMP);
		boolean exist = ZooKeeperUtils.exist(binlogPositionPath);
		if (state.equals(MyConstants.UPDATE)) {// 绝对要按照task的值来
			fn = taskJsonObject.getString(MyConstants.BINLOG_FILE_NAME);
			String position = taskJsonObject.getString(MyConstants.BINLOG_FILE_POSITION).trim();
			fp = position.length() == 0 ? 0 : Long.parseLong(position);
			// 映射到ZK
			if (exist) {
				ZooKeeperUtils.update(binlogPositionPath, fn + ":" + fp + ":" + (taskTimeStamp + 1));// 务必反映到zk中
			} else {
				ZooKeeperUtils.createPersistent(binlogPositionPath, fn + ":" + fp + ":" + (taskTimeStamp + 1));// 务必反映到zk中
			}
			// 然后最重要的，必须修改当前值为UPDATE_DONE
			taskJsonObject.put(MyConstants.STATE, MyConstants.UPDATE_DONE);
			ZooKeeperUtils.update(taskPath, taskJsonObject.toJSONString());
		} else {// 增加
			if (exist) {// 存在，之前有跑过, 此时需要根据二者的timestamp来比较 //
				String binlogValue = ZooKeeperUtils.getData(binlogPositionPath);
				String[] binlogValues = binlogValue.split(":");
				long binlogTimeStamp = Long.parseLong(binlogValues[2]);
				LoggerUtils.debug(logger, "taskTimeStamp:" + taskTimeStamp + " binlogTimeStamp:" + binlogTimeStamp);
				// 取最新值为准,正常情况下，都是binlog最新，但是如果是强制修改，那就是task更新
				if (taskTimeStamp > binlogTimeStamp) {
					// 取task为准
					fn = taskJsonObject.getString(MyConstants.BINLOG_FILE_NAME);
					String position = taskJsonObject.getString(MyConstants.BINLOG_FILE_POSITION).trim();
					fp = position.length() == 0 ? 0 : Long.parseLong(position);
					// 映射到ZK
					ZooKeeperUtils.update(binlogPositionPath, fn + ":" + fp + ":" + System.currentTimeMillis());// 务必反映到zk中
					LoggerUtils.debug(logger, binlogPositionPath + " exist,but task is newer,continue based on task data");
				} else {
					// 以binlog为准
					fn = binlogValues[0];
					fp = Long.parseLong(binlogValues[1]);
					LoggerUtils.debug(logger, binlogPositionPath + " exist,continue from this point");
				}

			} else {// 不存在，直接拿取当前值
				JSONObject jsonObject = JSON.parseObject(taskData);
				fn = jsonObject.getString(MyConstants.BINLOG_FILE_NAME).trim();
				String position = jsonObject.getString(MyConstants.BINLOG_FILE_POSITION).trim();
				fp = position.length() == 0 ? 0 : Long.parseLong(position);
				ZooKeeperUtils.createPersistent(binlogPositionPath, fn + ":" + fp + ":" + System.currentTimeMillis());// 务必反映到zk中
				LoggerUtils.debug(logger, binlogPositionPath + " not exist,create it,default value:" + fn + ":" + fp);
			}
		}

		// 4)尝试创建连接conn
		Connection conn = null;
		int lastIndex = taskPath.lastIndexOf("/");
		String target = taskPath.substring(lastIndex + 1);
		LoggerUtils.debug(logger, "target:" + target);
		String[] elements = target.split(":");
		String ip = elements[0];
		int port = Integer.valueOf(elements[1]);
		try {
			LoggerUtils.info(logger, "target machine:" + ip + ":" + port);
			long clientID = Long.parseLong(taskJsonObject.getString(MyConstants.CLIENTID));
			{
				// 只有这里加入元数据信息了，否则netty大量等待获取meta,会非常非常非常慢！！！
				// 这里全量拉取，后面按需更新即可
				HashMap<String, HashMap<String, String>> metaMapping = MetaUtils.getAllMetaInformationsPerConnection(//
						ip, //
						port, //
						taskJsonObject.getString(MyConstants.USERNAME_FOR_SCHEMA), //
						taskJsonObject.getString(MyConstants.PASSWORD_FOR_SCHEMA)//
				);
				HashMap<String, ArrayList<String>> pkMapping = MetaUtils.getAllPimaryKeyInformationsPerConnection(ip,
						port, //
						taskJsonObject.getString(MyConstants.USERNAME_FOR_SCHEMA), //
						taskJsonObject.getString(MyConstants.PASSWORD_FOR_SCHEMA)//
				);
				if (null == metaMapping) {
					throw new Exception("metaMapping is null...");
				}
				if (null == pkMapping) {
					throw new Exception("pkMapping is null...");
				}
				// 准备好连接
				conn = ConnectionFactory.makeObject(ip, port, taskData, currentPath, binlogPositionPath, fn, fp, clientID);
				conn.getAttributes().setDatabaseTableColumnsMapping(metaMapping);
				conn.getAttributes().setPrimaryKeysMapping(pkMapping);
			}

		} catch (Exception e) {
			LoggerUtils.error(logger, e.toString());
			if (null != conn) {
				conn.close();// 及时关闭
			}
			conn = null;
		}
		// conn = null;
		if (null != conn) {// 创建成功
			LoggerUtils.debug(logger, "create socket succeed: " + conn.getSocketChannel());
			// 放入到netty管理体系
			NettyQueue.addObject(conn);
			// 1)等待结果,放进去，不代表一定没有问题，需要监督确实被netty管理了
			// 什么情况下？比如mysql服务器主动关闭了，这一个情况需要主动探知
			while (0 == conn.getAttributes().getNettyManageState()) {
				TimeUtils.sleepMilliSeconds(10);// 如果为0就一直等待
			}
			// 2)已经有结果了，且为失败
			if (-1 == conn.getAttributes().getNettyManageState()) {
				// 表明有情况，比如mysql主动关闭连接了
				LoggerUtils.error(logger, "create conn socket failed by netty management system...");
				ZooKeeperUtils.deletePath(currentPath);// 创建失败的话,强制删除running节点,然后大家再一起竞争
			} else {
				LoggerUtils.info(logger, "create conn socket succeed by netty management system...");
			}
		} else {
			LoggerUtils.error(logger, "create conn socket failed");
			ZooKeeperUtils.deletePath(currentPath);// 创建失败的话,强制删除running节点,然后大家再一起竞争
		}

	}

	static {// 仅仅执行1次
		new Thread(new Runnable() {
			// 目前进程内的任务数初始化为0
			private AtomicInteger tasks = new AtomicInteger(0);
			//
			//
			//
			// 本地存储的全局机器-任务数---比较器
			Comparator<Machine> OrderComparator = new Comparator<Machine>() {
				public int compare(Machine machine1, Machine machine2) {
					//
					int task1 = machine1.getTaskNumber();
					int task2 = machine2.getTaskNumber();
					if (task1 < task2) {
						return -1;
					} else if (task1 == task2) {
						return 0;
					} else {
						return 1;
					}
				}

			};
			//// 本地存储的全局机器-任务数---优先级队列
			private PriorityQueue<Machine> machinePriorityQueue = new PriorityQueue<Machine>(10, OrderComparator);
			private String key = MyConstants.ZK_NAMESPACE_MACHINES + "/" + IpUtils.getServerId();

			@Override
			public void run() {
				// 更新本地集合&广而告之
				tasks.set(0);
				Machine localMachine = new Machine(IpUtils.getServerId(), 0);
				machinePriorityQueue.add(localMachine);
				ZooKeeperUtils.upsertEphemeral(key, "0");
				//
				while (true) {
					// 开始处理各种操作事件
					OpeationEvent event = OperationQueue.getObject();
					if (null == event) {
						TimeUtils.sleepMilliSeconds(50);
						continue;
					}
					// 拿到事件了
					switch (event.getType()) {
					case TASK_ADD:// 增加了一个任务，自然也需要处理它
						String taskPath = event.getEvent().getData().getPath();
						String taskData = new String(event.getEvent().getData().getData());
						AddTask(taskPath, taskData, true, machinePriorityQueue);
						break;
					case TASK_UPDATE:// 修改了一个任务
						// 需要做什么?
						// 1)如果当前已经有对应的连接在跑,主动关闭它,
						// 就会触发RUNNING_DELETE-->
						// ->AddTask(taskPath,newString(data), false);
						taskPath = event.getEvent().getData().getPath();
						taskData = new String(event.getEvent().getData().getData());
						int lastIndex = taskPath.lastIndexOf("/");
						String target = taskPath.substring(lastIndex + 1);
						String[] elements = target.split(":");
						String ip = elements[0];
						int port = Integer.valueOf(elements[1]);
						// 直接触发对应的socket关闭就可以
						MyNioSocketChannel mySocketChannel = ConnectionFactory.get(StringUtils.union(ip, "" + port));
						if (null != mySocketChannel) {// 触发关闭
							NettyUtils.triggerChannelClosed(mySocketChannel);
							// 然后需要确保netty全部处理结束了才会处理下一个事件
							while (null != ConnectionFactory.get(StringUtils.union(ip, "" + port))) {
								TimeUtils.sleepMilliSeconds(50);
							}
							LoggerUtils.info(logger, "netty close environment [" + ip + ":" + port + "] successfully...bingo");
						} else {// 直接尝试抢占
							AddTask(taskPath, taskData, false, machinePriorityQueue);
						}
						break;
					case RUNNING_DELETE:// 删除了，有人退出了，自然也需要处理它，可以巧妙的转换为新增任务
						// 删除有2种可能:
						// 1)正常删除，可以继续竞争
						// 2)停止任务，父节点都没了
						// 可能有必要暂停5秒
						TimeUtils.sleepSeconds(5);// 停止5秒,以便观察到底是正常删除还是停止任务
						// 观察结束
						LoggerUtils.debug(logger, Thread.currentThread() + "now let us handler running node deleted ");
						final String currentPath = event.getEvent().getData().getPath();// kmonitor-mysql-binlog/tasks/ip:3306/running/current
						int endIndex = currentPath.lastIndexOf("/running");
						taskPath = currentPath.substring(0, endIndex);
						if (ZooKeeperUtils.exist(taskPath)) {// 存在，说明是正常删除,正常竞争
							byte[] data = ZooKeeperUtils.getData(taskPath).getBytes();
							AddTask(taskPath, new String(data), false, machinePriorityQueue);
						} else {// 不存在，说明是停止任务,谁存在谁负责销毁
							// 并且不要再去触发task的争抢了
							// 首先要取消对这个runningPath的监听
							ZooKeeperUtils.deRegisterChildListener(taskPath + "/running",
									RunningListener.getInstance().getListener());
							// 然后可以开始处理逻辑了
							lastIndex = taskPath.lastIndexOf("/");
							target = taskPath.substring(lastIndex + 1);
							elements = target.split(":");
							ip = elements[0];
							port = Integer.valueOf(elements[1]);
							mySocketChannel = ConnectionFactory.get(StringUtils.union(ip, "" + port));
							if (null != mySocketChannel) {// 如果当前机器上有任务在跑的话就停止它，然后什么都不做
								LoggerUtils.debug(logger, "succeed to find the memory MyNioSocketChannel mySocketChannel");
								NettyUtils.triggerChannelClosed(mySocketChannel);
								// 然后需要确保netty全部处理结束了才会处理下一个事件
								while (null != ConnectionFactory.get(StringUtils.union(ip, "" + port))) {
									TimeUtils.sleepMilliSeconds(50);
								}
								LoggerUtils.info(logger, "netty close environment [" + ip + ":" + port + "] successfully...bingo");
							} else {// 否则什么都不做
								// 都停止了，就不要做啥了
							}
						}
						break;

					case SOCKET_ADD:
						LoggerUtils.debug(logger, "socket increased,will update the value of serverId under .../machines");
						// 更新数字
						tasks.set(tasks.intValue() + 1);
						// 更新本地集合
						Machine m = new Machine(IpUtils.getServerId(), tasks.get());
						machinePriorityQueue.remove(m);
						machinePriorityQueue.add(m);
						// 广而告之
						ZooKeeperUtils.upsertEphemeral(key, "" + tasks.get());
						break;
					case SOCKET_DELETE:
						LoggerUtils.debug(logger, "socket decreased,will update the value of serverId under .../machines");
						// 更新数字
						tasks.set(tasks.intValue() - 1);
						// 更新本地集合
						m = new Machine(IpUtils.getServerId(), tasks.get());
						machinePriorityQueue.remove(m);
						machinePriorityQueue.add(m);
						// 广而告之
						ZooKeeperUtils.upsertEphemeral(key, "" + tasks.get());
						break;
					//
					//
					//
					// 开始处理MACHINE的三种信息变化
					case MACHINE_ADD:
					case MACHINE_UPDATE:
						// 有新的机器增加了
						LoggerUtils.debug(logger, "some machine add/updated..." + event.getType());
						//
						String machinePath = event.getEvent().getData().getPath();
						lastIndex = machinePath.lastIndexOf("/");
						machinePath = machinePath.substring(lastIndex + 1);
						// 如果是自己的信息，则屏蔽
						if (null != machinePath && machinePath.length() > 0 && machinePath.equals(IpUtils.getServerId())) {
							// 自己发出的，屏蔽
							LoggerUtils.debug(logger, "sent by myself,omit");
							break;
						}
						String machineData = new String(event.getEvent().getData().getData());
						LoggerUtils.debug(logger, "path: " + machinePath + " data: " + machineData);
						Machine machine = new Machine(machinePath, Integer.parseInt(machineData));
						// 开始添加,但是之前是否存在呢？
						// 先删除再加// 这样才可以确保唯一性
						machinePriorityQueue.remove(machine);
						machinePriorityQueue.add(machine);
						LoggerUtils.debug(logger, "current PriorityQueue: " + machinePriorityQueue);
						break;
					case MACHINE_DELETE:
						// 有新的机器增加了
						LoggerUtils.debug(logger, "some machine delete...");
						machinePath = event.getEvent().getData().getPath();
						lastIndex = machinePath.lastIndexOf("/");
						machinePath = machinePath.substring(lastIndex + 1);
						// 如果是自己的信息，则屏蔽
						if (null != machinePath && machinePath.length() > 0 && machinePath.equals(IpUtils.getServerId())) {
							// 自己发出的，屏蔽,实际上这种情况不会出现
							LoggerUtils.debug(logger, "sent by myself,omit");
							break;
						}
						machineData = "0";// 数组不需要参加比较,所以可以随便填写
						LoggerUtils.debug(logger, "path: " + machinePath + " data: " + machineData);
						machine = new Machine(machinePath, Integer.parseInt(machineData));
						// 尽一切可能删除就行了,虽然有可能不在，至少保证删除,删除不再增加
						machinePriorityQueue.remove(machine);
						LoggerUtils.debug(logger, "current PriorityQueue: " + machinePriorityQueue);
						break;
					default:
						break;
					}
					// switch结束
				}
			}
		}).start();

	}

	public synchronized static void start() {
		// 仅仅为了启动上面的线程
	}

	// 内部类
	static class Machine {
		// 成员变量
		private String serverId;
		private int taskNumber;

		public Machine(String sid, int t) {
			this.serverId = sid;
			this.taskNumber = t;
		}

		public String getServerId() {
			return serverId;
		}

		public void setServerId(String serverId) {
			this.serverId = serverId;
		}

		public int getTaskNumber() {
			return taskNumber;
		}

		public void setTaskNumber(int taskNumber) {
			this.taskNumber = taskNumber;
		}

		// 不这样写就不能执行equals方法
		@Override
		public boolean equals(Object m) {
			// 只要serverId一致就是同样的
			if (null == m) {
				return false;
			}
			return serverId.equals(((Machine) m).getServerId());
		}

		@Override
		public String toString() {
			return "Machine [serverId=" + serverId + ", taskNumber=" + taskNumber + "]";
		}

	}
}
