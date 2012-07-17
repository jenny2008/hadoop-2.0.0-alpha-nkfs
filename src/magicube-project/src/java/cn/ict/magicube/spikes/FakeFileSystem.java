package cn.ict.magicube.spikes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.net.NetUtils;

public class FakeFileSystem extends FilterFSLV2 {
	static final private Log LOG = LogFactory.getLog(FakeFileSystem.class);
	
	static  {
		String log4jproperties;
		log4jproperties = 
			Thread.currentThread().getContextClassLoader().
			getResource("log4j.properties").toString();
		System.out.println(log4jproperties);
		String lsclass = 
			Thread.currentThread().getContextClassLoader().
			getResource("org/apache/hadoop/fs/shell/Ls.class").toString();
		System.out.println(lsclass);
	}
	
	private URI convertURIToBase(URI src) {
		URI res;
		try {
			assert src.getScheme() == "fake";
			res = new URI("file", src.getSchemeSpecificPart(), null);
		} catch (URISyntaxException e) {
	        throw new IllegalArgumentException(e);
		}
		return res;
	}
	
	private URI convertURIToFake(URI src) {
		URI res;
		try {
			LOG.info("src = " + src);
			assert src.getScheme() == "file";
			res = new URI("fake", src.getSchemeSpecificPart(), null);
			LOG.info("res = " + res);

		} catch (URISyntaxException e) {
	        throw new IllegalArgumentException(e);
		}
		return res;
	}
	
	private Path convertPathToBase(Path p) {
		if ((p.toUri().getScheme() != null) && (p.toUri().getScheme() != "")) {
			p = new Path(convertURIToBase(p.toUri()));
		}
		return p;
	}
	
	private Path convertPathToFake(Path p) {
		if ((p.toUri().getScheme() != null) && (p.toUri().getScheme() != "")) {
			p = new Path(convertURIToFake(p.toUri()));
		}
		return p;
	}
	
	private FileStatus convertFileStatusToFake(FileStatus stat) {
		Path oldP = stat.getPath();
		Path newP = new Path(convertURIToFake(oldP.toUri()));
		FileStatus newfs = new FileStatus(
				stat.getLen(),
				stat.isDirectory(),
				stat.getReplication(),
				stat.getBlockSize(),
				stat.getModificationTime(),
				stat.getAccessTime(),
				stat.getPermission(),
				stat.getOwner(),
				stat.getGroup(),
				newP
		);
		return newfs;
	}
	
	public FakeFileSystem() {
		super(new RawLocalFileSystem());
	}

	public FileStatus[] listStatus(Path f) throws FileNotFoundException, 
		IOException {
		try {
			throw new Exception();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		LOG.info("called listStatus: " + f.toString());
		LOG.info(f.getName());
		LOG.info(fs.getClass());
		
		Path rawPath = new Path(convertURIToBase(f.toUri()));
		
		FileStatus[] res = fs.listStatus(rawPath);
		FileStatus[] real_res = new FileStatus[res.length];
		LOG.info("returned");
		int i = 0;
		for (FileStatus fs : res) {
			real_res[i] = convertFileStatusToFake(fs);
			i ++;
		}
		return real_res;
	}
	
	public FileStatus getFileStatus(Path f) throws IOException {
		if (f.toUri().getScheme() == "fake") {
			f = new Path(convertURIToBase(f.toUri()));
		}
	    return convertFileStatusToFake(fs.getFileStatus(f));
	}
	
	public Path makeQualified(Path path) {
		if (path.toUri().getScheme() != null)
			path = convertPathToBase(path);
		Path qPath = fs.makeQualified(path);
		//qPath = convertPathToFake(qPath);
		return qPath;
	}

	
	@Override
	public String getScheme() {
		LOG.info("called getScheme");
		return "fake";
	}
	
	@Override
	public URI getUri() {
		LOG.info("called getUri");
		try {
			return new URI("fake:///");
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
}
