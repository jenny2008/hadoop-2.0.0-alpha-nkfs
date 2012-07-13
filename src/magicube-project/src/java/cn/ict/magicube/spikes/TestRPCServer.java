package cn.ict.magicube.spikes;


import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configurable;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;
import java.net.URL;

public class TestRPCServer implements TestRPCServerProtocol {
	
	private Server server = null;
	private Configuration conf = null;
	
	public static final Log LOG = LogFactory.getLog(TestRPCServer.class.toString());
	static {
	    Configuration.addDefaultResource("core-site.xml");
	}
	
	@Override
	public int getNum(int x) {
		LOG.info(String.format("received RPC call, x = %d", x));
		return x * 2;
	}
	
	
	public void join() {
		try {
			if (server != null)
				server.join();
		} catch (Throwable e) {
			/* do nothing */
		}
	}
	
	public TestRPCServer(String[] args) throws IOException {
		conf = new Configuration();
		//LOG.info("test getconf");
		//LOG.info(conf.get("fs.default.name"));
		//conf.writeXml(System.out);
		/* start the server */
		server = RPC.getServer(this.getClass(), this, "localhost", 12345, 10, true, 
				conf, null, null);
		server.start();
		LOG.info("server started");
	}
	
	
	public static TestRPCServer createServer(String[] args) {
		try {
			return new TestRPCServer(args);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	public static void main(String[] args) {
		LOG.info("start!");
		//URL url = Class.class.getClassLoader().getResource("core-site.xml");
		URL url = Thread.currentThread().getContextClassLoader().getResource("log4j.properties");
		LOG.info(url.toString());
	    try {
	        StringUtils.startupShutdownMessage(TestRPCServer.class, args, LOG);
	        TestRPCServer server = createServer(args);
	        if (server != null) {
	        	server.join();
	        }
	    } catch (Throwable e) {
	    	LOG.error(StringUtils.stringifyException(e));
	        System.exit(-1);
	    }
	}


	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
			throws IOException {
		if (protocol.equals(TestRPCServer.class.getName())) {
		      return TestRPCServerProtocol.versionID;
		} else {
			throw new IOException("Unknown protocol to name node: " + protocol);
		}
	}


	@Override
	public ProtocolSignature getProtocolSignature(String protocol,
			long clientVersion, int clientMethodsHash) throws IOException {
		// TODO Auto-generated method stub
	    return ProtocolSignature.getProtocolSignature(
	            this, protocol, clientVersion, clientMethodsHash);
	}
}
