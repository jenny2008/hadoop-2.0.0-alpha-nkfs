package cn.ict.magicube.spikes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.net.URL;

public class TestRPCServer implements TestRPCServerProtocol {
	public static final Log LOG = LogFactory.getLog(TestRPCServer.class.toString());

	@Override
	public int getNum(int x) {
		LOG.info(String.format("received RPC call, x = ", x));
		return x * 2;
	}
	
	public static void main(String[] args) {
		LOG.info("start!");
		//URL url = Class.class.getClassLoader().getResource("core-site.xml");
		URL url = Thread.currentThread().getContextClassLoader().getResource("log4j.properties");
		LOG.info(url.toString());
	}
	
}
