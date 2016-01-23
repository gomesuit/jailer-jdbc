package jailer.jdbc.model;

import jailer.core.model.ConnectionInfo;
import jailer.core.model.ConnectionKey;

public class ConnectionKeyData extends ConnectionKey{
	private ConnectionInfo info;

	public ConnectionInfo getInfo() {
		return info;
	}
	public void setInfo(ConnectionInfo info) {
		this.info = info;
	}

	@Override
	public String toString() {
		return "ConnectionKeyData [info=" + info + ", toString()="
				+ super.toString() + "]";
	}
}
