package com.hzw.monitor.mysqlbinlog.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * 工具类
 * 
 * @author gqliu 2015年11月6日
 *
 */
public class IpUtils {

	private static String fetchServerId() {
		String hostname = "UNKNOW";
		String ip = null;
		String pid = "UNKNOWUNKNOWUNKNOW";
		try {
			String[] str = ManagementFactory.getRuntimeMXBean().getName().split("@");
			hostname = str[1];
			pid = str[0];
			hostname = hostname.replaceAll(":", "_");
		} catch (Exception e) {
		}
		// 获取ip
		Runtime run = Runtime.getRuntime();
		BufferedReader br = null;
		Process p = null;
		try {
			p = run.exec(new String[] { "/bin/bash", "-c", "ifconfig -a|grep inet|grep -v inet6|grep -v 127.0.0.1" });
			br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			ip = br.readLine().trim().split("\\s+|:")[2];
			ip = ip.replaceAll(":", "_");
			p.waitFor();
		} catch (Exception e) {
		} finally {
			if (br != null)
				try {
					br.close();
				} catch (IOException e) {
				}
			if (p != null)
				p.destroy();
		}
		if (ip == null || "".equals(ip.trim())) {
			ip = hostname;
		}

		// 获取tcp端口
		List<Integer> ports = getPortByCmd(
				new String[] { "/bin/bash", "-c", "netstat -antp|grep LISTEN |grep ' " + pid + "/'" });
		// 如果tcp端口为空则获取udp
		if (ports == null || ports.size() <= 0) {
			ports = getPortByCmd(new String[] { "/bin/bash", "-c", "netstat -anup | grep ' " + pid + "/'" });
		}

		String port = ports.toString().replaceAll("\\s*", "");
		return ip + "-" + port;
	}

	private static List<Integer> getPortByCmd(String[] cmd) {
		// 先获取tcp端口
		Runtime run = Runtime.getRuntime();
		BufferedReader br = null;
		Process p = null;
		List<Integer> ports = new LinkedList<Integer>();
		try {
			p = run.exec(cmd);
			br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String str = null;
			while ((str = br.readLine()) != null) {
				try {
					str = str.trim().split("\\s+|\t+")[3].trim();
					str = str.substring(str.lastIndexOf(":") + 1);
					ports.add(Integer.parseInt(str));
				} catch (Exception e) {
				}
			}
			Collections.sort(ports);

			p.waitFor();
		} catch (Exception e) {
		} finally {
			if (br != null)
				try {
					br.close();
				} catch (IOException e) {
				}
			if (p != null)
				p.destroy();
		}
		return ports;
	}

	/// 静态函数
	private static String serverId = null;

	static {
		serverId = fetchServerId();
	}

	public static String getServerId() {
		return serverId;
	}

	// public static long getIdFromIPS(String[] ipArray) {
	// Long[] result = new Long[4];
	// result[0] = (long) 0;
	// result[1] = (long) 0;
	// result[2] = (long) 0;
	// result[3] = (long) 0;
	// String[] numberArray;
	// for (String ip : ipArray) {
	// numberArray = ip.split("\\.");
	// result[0] += Long.parseLong(numberArray[0]);
	// result[1] += Long.parseLong(numberArray[1]);
	// result[2] += Long.parseLong(numberArray[2]);
	// result[3] += Long.parseLong(numberArray[3]);
	// }
	// // 构造
	// return Long.parseLong(result[0] + "" + result[1] + "" + result[2] + "" +
	// result[3]);
	// }

	// public static long getClientIdFromStrHashCode(String s) {
	// int code = s.hashCode();
	// // 修正
	// if (code < 0) {
	// code += Integer.MAX_VALUE;
	// }
	// return code;
	// }

	// test
	public static void main(String[] args) {
		String testStr = "172.172.12.34,23.45.67.89";
		System.out.println(testStr.hashCode());
		//System.out.println(getClientIdFromStrHashCode(testStr));
		// System.out.println(getIdFromIPS(testStr.split(",")));
	}
}
