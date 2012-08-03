package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import cn.ict.magicube.fs.ParityCreator;

public class HDFSParityCreator extends ParityCreator {
	public static final Log LOG = LogFactory.getLog(HDFSParityCreator.class);
	private final DistributedFileSystem _dfs;
	private final ClientProtocol _wrappedNamenode;
	private final ClientProtocol _unwrappedNamenode;
	private final DFSClient _dfsClient;
	private final Configuration _conf;
	private Set<DatanodeInfo> _excludedDatanodes;
	
	private URI getNamenodeURI() {
		URI uri = URI.create(_conf.get("nkfs.baseFSURI", "hdfs:///"));
		assert (uri.getScheme().equals("hdfs"));
		String scheme = uri.getScheme();
		String authority = uri.getAuthority();
		if (authority == null) {                       // no authority
			URI defaultUri = FileSystem.getDefaultUri(_conf);
			if (scheme.equals(defaultUri.getScheme())    // if scheme matches default
					&& defaultUri.getAuthority() != null) {  // & default has authority
				return defaultUri;
			}
		}
		return uri;
	}

	private String getPathName(Path file) {
		// makeAbsolute
		if (!file.isAbsolute()) {
			file = new Path(_baseFS.getWorkingDirectory(), file);
		}
		return file.toUri().getPath();
	}

	
	private ClientProtocol getNamenode(URI nameNodeUri) throws IOException {
		NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyInfo =
			NameNodeProxies.createProxy(_conf, nameNodeUri, ClientProtocol.class);
		return proxyInfo.getProxy();
	}
	
	private class WrapperInvocationHandler implements InvocationHandler {
		private final ClientProtocol _inner;
		public WrapperInvocationHandler(ClientProtocol inner) {
			_inner = inner;
		}
		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			Method addBlock = ClientProtocol.class.getMethod("addBlock",
					String.class,
					String.class,
					ExtendedBlock.class,
					DatanodeInfo[].class
					);
			if (method.equals(addBlock)) {
				// addBlock is called in a high concurrency way
				synchronized (_excludedDatanodes) {
					if (LOG.isDebugEnabled()) {
						if ((args != null) && (args[0] != null)) 
							LOG.debug("addBlock is called for " + args[0]);
						else
							LOG.debug("addBlock is called");
					}
					DatanodeInfo[] originEx = (DatanodeInfo[])(args[3]);
					if (originEx == null) {
						originEx = new DatanodeInfo[0];
					}
					for (DatanodeInfo d : originEx) {
						_excludedDatanodes.add(d);
						LOG.debug("add " + d + " to excluded datanode set (passed down)");
					}
					LocatedBlock result = null;
					DatanodeInfo[] newEx = new DatanodeInfo[_excludedDatanodes.size()];
					_excludedDatanodes.toArray(newEx);
					if (newEx.length > 0)
						args[3] = newEx;
					else
						args[3] = null;
					try {
						result = (LocatedBlock)method.invoke(_inner, args);
					} catch (java.lang.reflect.InvocationTargetException e) {
						LOG.warn("we require more datanodes");
						args[3] = originEx;
						result = (LocatedBlock)method.invoke(_inner, args);
					}
					
					_excludedDatanodes.addAll(
							Arrays.asList(result.getLocations())
					);
					
					if (LOG.isDebugEnabled()) {
						LOG.debug("_excludedDatanodes=:");
						for (DatanodeInfo d : _excludedDatanodes) {
							LOG.debug("\t" + d.toString());
						}
					}
					return result;				
				}
			}
			return method.invoke(_inner, args);
		}
	}
	private ClientProtocol wrapNamenode(ClientProtocol namenode) {
		InvocationHandler wrapperHandler = new WrapperInvocationHandler(namenode);
		ClientProtocol proxy = (ClientProtocol)Proxy.newProxyInstance(
				WrapperInvocationHandler.class.getClassLoader(),
				new Class[] { ClientProtocol.class },
				wrapperHandler);
		return proxy;
	}
	
	protected HDFSParityCreator(DistributedFileSystem baseFS, Path partDirPath) throws Exception {
		super(baseFS, partDirPath);
		LOG.debug("creating HDFSParityCreator");
		_dfs = baseFS;
		_conf = _dfs.getConf();
		URI nameNodeUri = getNamenodeURI();
		LOG.debug("namenode is " + nameNodeUri);
		_unwrappedNamenode = getNamenode(nameNodeUri);
		_wrappedNamenode = wrapNamenode(_unwrappedNamenode);
		_dfsClient = new DFSClient(null, _wrappedNamenode, _conf,
				FileSystem.getStatistics(nameNodeUri.getScheme(),
						_dfs.getClass()));
		reset();
	}

	private OutputStream originOS = null;
	private List<OutputStream> paritiesOSs = null;

	@Override
	public void addOutputPath(Path filePath, boolean isOrigin)
			throws IOException {
		String pathName = getPathName(filePath);
		if (_dfsClient.exists(pathName)) {
			_dfsClient.delete(pathName, true);
		}
		OutputStream os = _dfsClient.create(
				getPathName(filePath),
				true,
				(short)1,
				_baseFS.getDefaultBlockSize()
				);
	    if (isOrigin) {
	    	if (originOS != null)
	    		originOS.close();
	    	originOS = os;
	    } else {
	    	paritiesOSs.add(os);
	    }
	}

	@Override
	public OutputStream[] getOutputStreams() throws IOException {
		OutputStream[] result = new OutputStream[paritiesOSs.size()];
		paritiesOSs.toArray(result);
		return result;
	}

	@Override
	public OutputStream getOriginOutputStream() throws IOException {
		return originOS;
	}

	@Override
	public void reset() {
		LOG.debug("reset HDFSParityCreator");
		_excludedDatanodes = new HashSet<DatanodeInfo>();
		try {
			FileStatus[] stats = _dfs.listStatus(_partDirPath);
			for (FileStatus stat : stats) {
				LOG.debug("existing file: " + stat.getPath());
				LocatedBlocks lbs = DFSClient.callGetBlockLocations(_unwrappedNamenode, 
						getPathName(stat.getPath()), 0, stat.getLen());
				for (LocatedBlock lb : lbs.getLocatedBlocks()) {
					DatanodeInfo[] datanodes = lb.getLocations();
					for (DatanodeInfo d : datanodes) {
						LOG.debug("add " + d + " to excluded datanode set (existing file)");
						_excludedDatanodes.add(d);
					}
				}
			}
			
			originOS = null;
			paritiesOSs = new LinkedList<OutputStream>();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
