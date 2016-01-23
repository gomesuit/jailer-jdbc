package jailer.jdbc;

public class DisConnectedNodeIdManager {
	private static long connectionId = 1L;
	
	synchronized public static String getId(String prefix){
		return prefix + "-" + connectionId++;
	}
}
