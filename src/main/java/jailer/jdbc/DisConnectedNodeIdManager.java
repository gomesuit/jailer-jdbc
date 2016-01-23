package jailer.jdbc;

public class DisConnectedNodeIdManager {
	private static long connectionId = 10000000000L;
	
	synchronized public static String getId(){
		return String.valueOf(connectionId++);
	}
}
