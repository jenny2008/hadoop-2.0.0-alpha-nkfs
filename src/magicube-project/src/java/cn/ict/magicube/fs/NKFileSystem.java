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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Progressable;

public class NKFileSystem extends FileSystem {
	
	public static final Log LOG = LogFactory.getLog(NKFileSystem.class);
	private FileSystem _baseFS = null;
	private Path _workingDir = null;

	static final URI NAME = URI.create("nkfs:///");
	static {
		Configuration.addDefaultResource("core-site.xml");
	}

	public class NKPathTranslator {
		private String _abs = null;
		private FileSystem _baseFS = null;
		public NKPathTranslator(Path topPath) {
			Path qualifiedTopPath = makeQualified(topPath);
			String strpath = qualifiedTopPath.toUri().getPath();
			/* remove leading '/' */
			int i = 0;
			for (i = 0; i < strpath.length(); i++) {
				if (strpath.charAt(i) != Path.SEPARATOR_CHAR)
					break;
			}
			_abs = strpath.substring(i);
		}
		
		public Path getTopFSPath() {
			return new Path(Path.SEPARATOR + _abs);
		}
		
		public Path getOriginPath() {
			return PathUtils.resolvePath(PathUtils.BASE_ORIGIN_DIR, _abs);
		}

		public Path getShadowPath() {
			return PathUtils.resolvePath(PathUtils.BASE_SHADOW_DIR, _abs);
		}
		
		public Path getMetadataDirPath() {
			return PathUtils.makeDirPath(
					PathUtils.resolvePath(
							PathUtils.BASE_METADATA_DIR, _abs));
		}

		public Path getParityDirPath() {
			return PathUtils.makeDirPath(
					PathUtils.resolvePath(
							PathUtils.BASE_PARITIES_DIR, _abs));
		}

		public Path getParityPartDirPath(long start, long length) {
			String partFileName = String.format("part-%d-%d", start, length);
			return PathUtils.makeDirPath(PathUtils.resolvePath(getParityDirPath(),
					partFileName));
		}
		
		public boolean isRaidedFile() throws IOException {
			FileStatus s_shadow = _baseFS.getFileStatus(getShadowPath());
			if (!s_shadow.isFile())
				return false;
			if (_baseFS.exists(getOriginPath()))
				return false;
			if (_baseFS.exists(getMetadataDirPath()))
				return true;
			throw new IOException("nkfs corrupted: check path " + getShadowPath());

		}
	}

	
	
	
	////////////////
	// trivial stuffs
	////////////////
	
	public FileSystem getBaseFS() {
		return _baseFS;
	}
	
	@Override
	public URI getUri() {
		return NAME;
	}
	
	@Override
	public String getScheme() {
		return "nkfs";
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
	@Override
	public void setWorkingDirectory(Path new_dir) {
		_workingDir = this.makeQualified(new_dir);
	}

	@Override
	public Path getWorkingDirectory() {
		return _workingDir;
	}
	
	@Override
	public void setPermission(Path p, FsPermission perm) throws IOException {
		NKPathTranslator ptran = new NKPathTranslator(p);
		_baseFS.setPermission(ptran.getShadowPath(), perm);
		return;
	}

	/////////////
	// Layout, dir, fileStatus stuff
	/////////////
	
	public NKFileSystem() throws IOException {
		super();
		_workingDir = new Path("/");
		LOG.debug("nkfs object created");
	}
	
	@Override
	public void initialize(URI name, Configuration conf) throws IOException {
		LOG.debug("NKFS is initialized: uri = " + name.toString());
		super.initialize(name, conf);
		this.setConf(conf);
		if (this.getConf() == null)
			this.setConf(new Configuration());

		PathUtils.initialize(this.getConf());
		String baseFSURI = conf.get("nkfs.baseFSURI", "hdfs:///");
		this._baseFS = FileSystem.get(URI.create(baseFSURI), this.getConf());
		LOG.debug("baseFS of NKFS is " + _baseFS.getClass());
		createBaseFSLayout();
	}

	private void createBaseFSLayout() throws IOException {
		LOG.debug("createBaseFSLayout");
		_baseFS.mkdirs(PathUtils.BASE_METADATA_DIR);
		_baseFS.mkdirs(PathUtils.BASE_SHADOW_DIR);
		_baseFS.mkdirs(PathUtils.BASE_ORIGIN_DIR);
		_baseFS.mkdirs(PathUtils.BASE_PARITIES_DIR);
		LOG.debug("layout OK");

	}

	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		LOG.debug("getFileStatus(" + f + ")");
		checkPath(f);
		/* check shadow dir */
		Path baseShadowPath = new NKPathTranslator(f).getShadowPath();
		LOG.debug("getFileStatus: check baseFS: " + baseShadowPath);
		FileStatus baseStatus = _baseFS.getFileStatus(baseShadowPath);
		LOG.debug("getFileStatus: baseFS returns: " + baseStatus);
		return new NKFileStatus(this, _baseFS, baseStatus, makeQualified(f));
	}
	
	@Override
	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		LOG.debug("mkdirs: " + f);
		NKPathTranslator ptran = new NKPathTranslator(f);
		try {
			_baseFS.mkdirs(ptran.getOriginPath(), permission);
			_baseFS.mkdirs(ptran.getMetadataDirPath(), permission);
			_baseFS.mkdirs(ptran.getParityDirPath(), permission);
			_baseFS.mkdirs(ptran.getShadowPath(), permission);
			return true;
		} catch (IOException e) {
			_baseFS.delete(ptran.getOriginPath(), true);
			_baseFS.delete(ptran.getMetadataDirPath(), true);
			_baseFS.delete(ptran.getParityDirPath(), true);
			_baseFS.delete(ptran.getOriginPath(), true);
			e.printStackTrace();
			throw e;
		}
	}

	public FileStatus[] listStatus(Path f) throws FileNotFoundException,
								IOException {
		LOG.debug(String.format("listStatus(%s)", f.toString()));
		NKPathTranslator ptran = new NKPathTranslator(f);

		FileStatus[] baseStatus = _baseFS.listStatus(ptran.getShadowPath());
		FileStatus[] resStatus = new FileStatus[baseStatus.length];

		int i = 0;
		for (FileStatus stat : baseStatus) {
			FileStatus r = new NKFileStatus(this, _baseFS, stat,
					PathUtils.convertFromBaseShadow(stat.getPath()));
			resStatus[i++] = r;
			LOG.info(r.getPath());
		}
		return resStatus;
	}


	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		LOG.debug("rename: " + src + " to " + dst);
		NKPathTranslator ptran_src = new NKPathTranslator(src);
		NKPathTranslator ptran_dst = new NKPathTranslator(dst);
		
		FileStatus s_shadow = _baseFS.getFileStatus(ptran_src.getShadowPath());
		if (s_shadow.isDirectory()) {
			/* simple situation */
			_baseFS.rename(ptran_src.getMetadataDirPath(), ptran_dst.getMetadataDirPath());
			_baseFS.rename(ptran_src.getOriginPath(), ptran_dst.getOriginPath());
			_baseFS.rename(ptran_src.getParityDirPath(), ptran_dst.getParityDirPath());
			_baseFS.rename(ptran_src.getShadowPath(), ptran_dst.getShadowPath());
			return true;
		}
		
		/* rename a file */
		/* first: check shadow path */
		assert s_shadow.isFile() : s_shadow.getPath().toString() + " should be a regular file";
		if (!ptran_src.isRaidedFile()) {
			_baseFS.rename(ptran_src.getOriginPath(), ptran_dst.getOriginPath());
			_baseFS.rename(ptran_src.getShadowPath(), ptran_dst.getShadowPath());
			return true;
		}
		
		assert (_baseFS.exists(ptran_src.getParityDirPath()));
		assert (_baseFS.exists(ptran_src.getMetadataDirPath()));
		
		_baseFS.rename(ptran_src.getMetadataDirPath(), ptran_dst.getMetadataDirPath());
		_baseFS.rename(ptran_src.getOriginPath(), ptran_dst.getOriginPath());
		_baseFS.rename(ptran_src.getShadowPath(), ptran_dst.getShadowPath());
		return true;
	}
	
	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {
		LOG.debug("deleting path " + f);
		NKPathTranslator ptran = new NKPathTranslator(f);
		FileStatus s_shadow = _baseFS.getFileStatus(ptran.getShadowPath());
		
		if (s_shadow.isDirectory()) {
			_baseFS.delete(ptran.getMetadataDirPath(), true);
			_baseFS.delete(ptran.getOriginPath(), true);
			_baseFS.delete(ptran.getParityDirPath(), true);
			_baseFS.delete(ptran.getShadowPath(), true);
			return true;
		}
		
		if (ptran.isRaidedFile()) {
			_baseFS.delete(ptran.getOriginPath(), false);
			_baseFS.delete(ptran.getShadowPath(), false);
			return true;
		}
		
		_baseFS.delete(ptran.getMetadataDirPath(), true);
		_baseFS.delete(ptran.getParityDirPath(), true);
		_baseFS.delete(ptran.getShadowPath(), true);
		return true;
	}

	@Override
	public FSDataOutputStream create(Path f, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		LOG.debug("creating path " + f);
		NKPathTranslator ptran = new NKPathTranslator(f);
		try {
			/* don't make metadata dir: indicate that the file still in origin */
			/* create an entry in shadow */
			LOG.debug("creating shadow: " + ptran.getShadowPath());
			
			/* if there is a file in shadow base, but not in original base,
			 * then overwrite is forbidden 
			 * */
			if (_baseFS.exists(ptran.getShadowPath())) {
				if (!_baseFS.exists(ptran.getOriginPath())) {
					throw new IOException("Overwriting a raided file is not allow");
				}
			}
			
			_baseFS.create(ptran.getShadowPath(),
					permission, overwrite, bufferSize, replication, blockSize,
					progress).close();
			/* create origin entry */
			LOG.debug("creating origin: " + ptran.getOriginPath());
			FSDataOutputStream os = _baseFS.create(ptran.getOriginPath(),
					permission, overwrite, bufferSize, replication, blockSize,
					progress);
			return os;
		} catch (IOException e) {
			LOG.warn("file creation failed");
			_baseFS.delete(ptran.getShadowPath(), false);
			_baseFS.delete(ptran.getOriginPath(), false);
			e.printStackTrace();
			throw e;
		}
	}

	@Override
	public FSDataOutputStream append(Path f, int bufferSize,
			Progressable progress) throws IOException {
		NKPathTranslator ptran = new NKPathTranslator(f);
		if (!ptran.isRaidedFile()) {
			return _baseFS.append(ptran.getOriginPath(), bufferSize, progress);
		}
		LOG.error("try to append to raided file " + f);
		throw new IOException("doesn't allow append to raided file");
	}
	
	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
