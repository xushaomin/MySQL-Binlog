package com.hzw.monitor.mysqlbinlog.test;

import java.util.concurrent.atomic.AtomicBoolean;

public class AtomicBooleanTest {
	
	public static void test(AtomicBoolean obj){
		obj.set(false);
		System.out.println(obj + " " + obj.hashCode());
	}
	public static void main(String[] args) {
		AtomicBoolean ref = new AtomicBoolean(false);
		AtomicBoolean b = ref;
		System.out.println(ref + " " + ref.hashCode());
		System.out.println(b + " " + b.hashCode());
		System.out.println("---");
		ref.set(true);
		System.out.println(ref + " " + ref.hashCode());
		System.out.println(b + " " + b.hashCode());
		//
		// if(a==b){
		// System.out.println("yes");
		// }
		// AtomicBoolean c=new AtomicBoolean(false);
		// System.out.println(c.hashCode());
		test(b);
		System.out.println(ref + " " + ref.hashCode());
	}
}
