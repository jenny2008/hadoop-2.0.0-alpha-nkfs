package cn.ict.magicube.fs.mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import cn.ict.magicube.fs.NKFileSystem;

public class NKFSUtil {
	public final NKFileSystem topFS;
	public final FileSystem baseFS;
	public final Configuration conf;
	public final int N;
	public final int K;
	
	static {
		Configuration.addDefaultResource("core-site.xml");
	}

	public NKFSUtil(Configuration conf) throws IOException {
		if (conf == null)
			conf = new Configuration();
		this.conf = conf;
		topFS = new NKFileSystem();
		topFS.initialize(URI.create("nkfs:///"), conf);
		baseFS = topFS.getBaseFS();
		
		K = conf.getInt("nkfs.parity.k", 3);
		N = conf.getInt("nkfs.parity.n", 5);
	}
	
	public NKFSUtil() throws IOException {
		this(null);
	}
}
