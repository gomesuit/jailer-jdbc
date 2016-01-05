package jailer.jdbc;

import jailer.core.PathManager;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class JailerJdbcURIManager {
	private static final String Prefix = "jdbc:";
	
	private static String getExcludePrefix(String url){
		return url.substring(Prefix.length());
	}

	public static URI getUri(String url) throws URISyntaxException {
		String strUri = getExcludePrefix(url);
		return new URI(strUri);
	}
	
	public static String getHost(URI uri){
		return uri.getHost();
	}
	
	public static int getPort(URI uri){
		return uri.getPort();
	}
	
	public static String getConnectString(URI uri){
		return uri.getAuthority();
	}
	
	public static String getPath(URI uri){
		return PathManager.getRootPath() + uri.getPath();
	}
	
	public static String getUUID(URI uri){
		return uri.getPath().substring(1);
	}
	
	public static Map<String, String> getParameterMap(URI uri){
		Map<String, String> resultMap = new HashMap<>();
		
		String query = uri.getQuery();
		
		if(query == null){
			return resultMap;
		}
		
		String[] keyValueList = query.split("&");
		for(String keyValue : keyValueList){
			String[] pair = keyValue.split("=");
			resultMap.put(pair[0], pair[1]);
		}
		return resultMap;
	}
}
