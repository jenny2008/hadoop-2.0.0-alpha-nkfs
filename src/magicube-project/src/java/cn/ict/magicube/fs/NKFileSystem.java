package cn.ict.magicube.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import cn.ict.magicube.spikes.FakeFileSystem2;

public class NKFileSystem extends FileSystem {
	static final URI NAME = URI.create("nkfs:///");
	static final Log LOG = LogFactory.getLog(NKFileSystem.class);
	private Configuration conf = null;

	
	static {
		Configuration.addDefaultResource("core-site.xml");
	}
	
	private FileSystem fs;
	private Path workingDir;
	
	static class NKFileStatus extends FileStatus {
		
	}

	public NKFileSystem() throws IOException {
		super();
		FileSystem.getFileSystemClass("hdfs://", conf);
	}
	
	@Override
	public URI getUri() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FSDataOutputStream create(Path f, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FSDataOutputStream append(Path f, int bufferSize,
			Progressable progress) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public FileStatus[] listStatus(Path f) throws FileNotFoundException,
			IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setWorkingDirectory(Path new_dir) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Path getWorkingDirectory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
