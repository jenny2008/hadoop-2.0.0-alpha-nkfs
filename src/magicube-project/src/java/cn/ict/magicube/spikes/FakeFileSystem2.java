package cn.ict.magicube.spikes;



import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.io.MD5Hash;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Progressable;

import com.sun.corba.se.spi.ior.MakeImmutable;

public class FakeFileSystem2 extends FileSystem {
	static final URI NAME = URI.create("fake2:///");
	static final Log LOG = LogFactory.getLog(FakeFileSystem2.class);
	static {
		Configuration.addDefaultResource("core-site.xml");
	}
	
	private FileSystem fs;
	private Path workingDir;
	
	private class LocalMap {
		private Path shadowPath = null;
		private Path metaDataPath = null;
		private Path[] dataPath = null;

		
		public LocalMap(Path p) {
			abs = new Path(makeQualified(p).toUri().
					getSchemeSpecificPart()).toString();
			
			/* remove leading '/' from abs */
			int i = 0;
			for (i = 0; i < abs.length(); i++) {
				if (abs.charAt(i) != Path.SEPARATOR_CHAR)
					break;
			}
			abs = abs.substring(i);
			Path absp = new Path(abs);
			assert(absp.getName() != "");
		}
		
		private String abs;
		public Path getShadowPath() {
			if (shadowPath != null) {
				return shadowPath;
			}
			shadowPath = fs.makeQualified(new Path("/tmp/fake2/shadow/", abs));
			return shadowPath;
		}
		
		public Path getMetaDataPath() {
			if (metaDataPath != null) {
				return metaDataPath;
			}
			String md5 = MD5Hash.digest(abs).toString();
			metaDataPath = fs.makeQualified(new Path("/tmp/fake2/meta/", md5));
			return metaDataPath;
		}
		
		public Path[] getDataPath() {
			if (dataPath != null) {
				return dataPath;
			}
			dataPath = new Path[2];
			dataPath[0] = fs.makeQualified(new Path("/tmp/fake2/data1/", abs));
			dataPath[1] = fs.makeQualified(new Path("/tmp/fake2/data2/", abs));
			return dataPath;
		}
	}
	

	
	static class Fake2FileStatus extends FileStatus {
		private static Path shadowPathToFake2(Path shadowPath) {
			URI rel = null;
			try {
				rel = new URI("file:///tmp/fake2/shadow/").relativize(
						shadowPath.toUri()
				);
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
			LOG.info("rel URI: " + rel.toString());
			Path absPath = null;
			try {
				absPath = new Path(new URI("fake2:///").resolve(rel));
				LOG.info("absPath is set to " + absPath.toUri());
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
			return absPath;
		}
		
		private boolean isPermissionLoaded() {
			return !super.getOwner().equals(""); 
		}

		private FileStatus baseFileStatus;

		private void loadPermissionInfo() {
			setPermission(baseFileStatus.getPermission());
			setOwner(baseFileStatus.getOwner());
			setGroup(baseFileStatus.getGroup());
			
		}

		Fake2FileStatus(FileStatus base, Path fake2Path) {
			super(base.getLen(), base.isDirectory(), base.getReplication(),
					base.getBlockSize(), base.getModificationTime(),
					fake2Path
					);
			
			this.baseFileStatus = base;
			LOG.info("create new Fake2FileStatus for " + fake2Path.toString());
			LOG.info("base is " + base);
			LOG.info("Path = " + this.getPath().toUri());
			//LOG.info("Qualified path = " + makeQualified(this.getPath()).toUri());
		}

		
		Fake2FileStatus(FileStatus shadowBase) {
			this(shadowBase, shadowPathToFake2(shadowBase.getPath()));
		}


		@Override
		public FsPermission getPermission() {
			if (!isPermissionLoaded()) {
				loadPermissionInfo();
			}
			return super.getPermission();
		}

		@Override
		public String getOwner() {
			if (!isPermissionLoaded()) {
				loadPermissionInfo();
			}
			return super.getOwner();
		}

		@Override
		public String getGroup() {
			if (!isPermissionLoaded()) {
				loadPermissionInfo();
			}
			return super.getGroup();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			if (!isPermissionLoaded()) {
				loadPermissionInfo();
			}
			super.write(out);
		}
	}

	public FakeFileSystem2() {
		super();
		fs = new RawLocalFileSystem();
		workingDir = new Path("/");

		try {
			fs.mkdirs(new Path("file:///tmp/fake2/shadow/"));
			fs.mkdirs(new Path("file:///tmp/fake2/meta/"));
			fs.mkdirs(new Path("file:///tmp/fake2/data1/"));
			fs.mkdirs(new Path("file:///tmp/fake2/data2/"));
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("failed to create local dirs");
		}
	}
	
	@Override
	public void initialize(URI name, Configuration conf) throws IOException {
		super.initialize(name, conf);
		this.setConf(conf);
		if (fs.getConf() == null) {
			fs.initialize(name, conf);
		}
	}
	
	@Override
	public URI getUri() {
		return NAME;
	}
	
	@Override
	protected URI getCanonicalUri() {
		return NetUtils.getCanonicalUri(getUri(), getDefaultPort());
	}
	
	@Override
	public Path makeQualified(Path path) {
		checkPath(path);
		return path.makeQualified(NAME, this.getWorkingDirectory());
	}
	

	public String getScheme() {
		return "fake2";
	}

	@Override
	public void setWorkingDirectory(Path new_dir) {
		workingDir = this.makeQualified(new_dir);
	}

	@Override
	public Path getWorkingDirectory() {
		return workingDir;
	}
	
	@Override
	/* ongoing... */
	public FileStatus getFileStatus(Path f) throws IOException {
		checkPath(f);
		LOG.info(String.format("getFileStatus: %s", f.toString()));
		LocalMap map = new LocalMap(f);
		
		/* check for shadow */

		LOG.info("checking shadowPath " + map.getShadowPath());
		fs.getDefaultBlockSize();
		FileStatus shadowStatus = fs.getFileStatus(map.getShadowPath());
		
		/* convert to fake2 FileStatus */
		FileStatus instanceStatus = new Fake2FileStatus(
				shadowStatus);
		return instanceStatus;
	}
	
	@Override
	/* untested */
	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		if(f == null) {
			throw new IllegalArgumentException("mkdirs path arg is null");
		}
		LocalMap map = new LocalMap(f);
		fs.mkdirs(map.shadowPath, permission);
		for (Path p : map.dataPath) {
			fs.mkdirs(p, permission);
		}
		return true;
	}
	
	@Override
	public FileStatus[] listStatus(Path f) throws FileNotFoundException,
			IOException {
		LOG.info(String.format("listStatus(%s)", f.toString()));
		LocalMap map = new LocalMap(f);
		LOG.info("look into " + map.getShadowPath());
		
		FileStatus[] stats = fs.listStatus(map.getShadowPath());
		FileStatus[] results = new FileStatus[stats.length];
		
		int i = 0;
		for (FileStatus stat : stats) {
			//Fake2FileStatus s = new Fake2FileStatus(stat, );
			LOG.info(String.format("File in localFS: %s", stat.getPath().toString()));
			results[i++] = new Fake2FileStatus(stat);
			LOG.info("build a new FileStatus: " + results[i-1].getPath());
		}
		
		return results;
	}

	
	////////////////////////////////
	// FileSystem
	////////////////////////////////
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





	

	/////////////////////////////////
	//
	/////////////////////////////////
	public static void main(String[] args) throws IOException, URISyntaxException {
		FileSystem fs = new FakeFileSystem2();
		fs.initialize(new URI("fake2:///"), new Configuration());
		Path p = fs.makeQualified(new Path("sd/path/to/file"));
		LOG.info("qulified path: " + p);
		fs.getFileStatus(new Path("fake2:///tmp/"));
	}
}
