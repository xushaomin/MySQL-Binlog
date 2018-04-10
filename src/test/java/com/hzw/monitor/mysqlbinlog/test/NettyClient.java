package com.hzw.monitor.mysqlbinlog.test;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class NettyClient {
	public static void main(String[] args) {
		Socket s;
		try {
			s = new Socket("127.0.0.1", 10001);
			s.close();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
