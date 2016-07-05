package com.blake.util;

public class SystemEnviroment {
	
	public static void getSystemInfo() {
		
		getMemoryInfo();
	}

 	public static void getMemoryInfo() {

       Runtime currRuntime = Runtime.getRuntime ();
       int nFreeMemory = ( int ) (currRuntime.freeMemory() / 1024 / 1024);
       int nTotalMemory = ( int ) (currRuntime.totalMemory() / 1024 / 1024);
       System. out .println( "Memory info: " + nFreeMemory + "M / " + nTotalMemory +"M(free/total)");
    }
}
