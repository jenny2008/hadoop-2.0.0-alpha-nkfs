package cn.ict.magicube.fs;

import java.io.IOException;
import java.util.LinkedList;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;


public class DoTransformNKFS {
	static final Log LOG = LogFactory.getLog(DoTransformNKFS.class);

	static {
		Configuration.addDefaultResource("core-site.xml");
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		NKFileSystem topFS = new NKFileSystem();
		topFS.initialize(URI.create("nkfs:///"), new Configuration());
		LinkedList<FileStatus> originFiles = new LinkedList<FileStatus>();
		NKFileSystem.PathTranslator ptran =
			topFS.new PathTranslator(new Path("/"));
		LOG.info("iterate over " + ptran.getBaseOriginPath());
		//topFS.l
	}

}
