package cn.ict.magicube.spikes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class TestRPCClient {
	
	public static final Log LOG = LogFactory.getLog(TestRPCClient.class.toString());
	static {
	    Configuration.addDefaultResource("core-site.xml");
	}

	
	public static void main(String[] args) {
		//RPC.call(method, params, addrs, ticket, conf)
		Configuration conf = new Configuration();
		InetSocketAddress addr = new InetSocketAddress("localhost", 12345);
		try {
			LOG.info("start!");
			TestRPCServerProtocol proxy =
				RPC.getProtocolProxy(TestRPCServerProtocol.class, 0, addr, conf).getProxy();
			LOG.info("proxy get!");
			int x = proxy.getNum(100);
			LOG.info("rpc result: " + x);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
