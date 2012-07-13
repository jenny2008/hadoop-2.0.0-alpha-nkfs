package cn.ict.magicube.spikes;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface TestRPCServerProtocol extends VersionedProtocol {
	public static final long versionID = 1L;
	public int getNum(int x);
}
