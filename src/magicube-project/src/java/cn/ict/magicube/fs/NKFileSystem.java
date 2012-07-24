package cn.ict.magicube.fs;

import java.io.DataInput;
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

/**
 * 
 * Design of NKFS:
 * 
 * split each file into pieces, piece size is: nkfs.piece.size
 * 
 * layout of base hdfs:
 * 
 * /base
 *   |- /origin    <-- original file (3 replications)
 *   |- /shadow    <-- tree of base fs
 *   |- /metadata      <-- metadata of files
 *        |- /full/path/of/file
 *            |- length
 *   |- /parities
 *        |- /full/path/of/file
 *             |- /pieces
 *                  |- /part_0001
 *                       origin
 *                       parity_0001
 *                       parity_0002
 *                       ...
 *                       parity_n
 *                     /part_0002
 *                       origin
 *                       parity_0001
 *                       parity_0002
 *                       ...
 *                       parity_n
 *
 *  configurations:
 *  
 *  nkfs.piece.sizeMB: size of a part of a file
 *  nkfs.parity.n
 *  nkfs.parity.k
 *  nkfs.basedir
 */
public class NKFileSystem extends FileSystem {
	static final Log LOG = LogFactory.getLog(NKFileSystem.class);

	public class PathTranslator {
		private String _abs;
		public PathTranslator(Path path) {
			Path qp = makeQualified(path);
			String strpath = qp.toUri().getSchemeSpecificPart();
			/* remove leading '/' */
			int i = 0;
			for (i = 0; i < strpath.length(); i++) {
				if (strpath.charAt(i) != Path.SEPARATOR_CHAR)
					break;
			}
			_abs = strpath.substring(i);
		}
		
		private String _digest = null;
		/*
		private String getDigest() {
			if (_digest != null)
				return _digest;
			_digest = MD5Hash.digest(_abs).toString();
			return _digest;
		}
		*/
		
		private Path resolvePath(Path p, String r) {
			/* don't "makeQualified". 
			 * -- OR --
			 * plus the final '/' after qualified URI
			 *  */
			return new Path(p.toUri().resolve(r));
		}
		
		public Path getBaseOriginPath() {
			return resolvePath(getBaseOriginDir(), _abs);
		}
		
		public Path getBaseShadowPath() {
			return resolvePath(getBaseShadowDir(), _abs);
		}
		
		public Path getBaseMetaDataPath() {
			return resolvePath(getBaseMetaDir(), _abs);

		}
		
		public Path getBaseParityPath() {
			return resolvePath(getBaseParityDir(), _abs);
		}
		
		
	}
	
	static final URI NAME = URI.create("nkfs:///");
	private Configuration conf = null;

	static {
		Configuration.addDefaultResource("core-site.xml");
	}
	
	private FileSystem baseFS;
	private Path workingDir;
	
	/* configuration of NKFS */
	
	/* nkfs.baseFSURI, default: "hdfs:///" */
	private String baseFSURI;
	/* nkfs.basedir, default: "/" */
	private Path basedir;
	/* nkfs.parity.n, default: 5 */
	private int N;
	/* nkfs.parity.k, default: 3 */
	private int K;
	/* nkfs.piece.sizeMB, default: 64 * 1024 * 1024 */
	private int pieceSize;
	
	private Path getBaseDir(String p) {
		URI uri = URI.create(
				basedir + Path.SEPARATOR + p + Path.SEPARATOR
		);
		return new Path(uri);
	}
	
	private Path getBaseMetaDir() {
		return getBaseDir("metadata");
	}
	
	private Path getBaseOriginDir() {
		return getBaseDir("origin");
	}

	private Path getBaseShadowDir() {
		return getBaseDir("shadow");
	}
	
	private Path getBaseParityDir() {
		return getBaseDir("parity");
	}

	public static Path convertFromBasePath(Path baseRoot, Path p) {
		URI base_root = URI.create(baseRoot.toUri().getSchemeSpecificPart());
		URI u = URI.create(p.toUri().getSchemeSpecificPart());
		String rel = Path.SEPARATOR + base_root.relativize(u).getSchemeSpecificPart();
		return new Path(rel);
	}
	
	public Path convertFromBaseShadow(Path p) {
		return convertFromBasePath(getBaseShadowDir(), p);
	}
	
	////////////////
	// NKFS specific stuffs
	////////////////
	FileSystem getBaseFileSystem() {
		return baseFS;
	}
	
	////////////////
	// trivial stuffs
	////////////////
	
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
		workingDir = this.makeQualified(new_dir);
	}

	@Override
	public Path getWorkingDirectory() {
		return workingDir;
	}
	
	class NKFileStatus extends FileStatus {
		private FileStatus _baseStatus;
		private Path _path;
		private long _length;
		public NKFileStatus(FileStatus baseStatus, Path path) {
			this._baseStatus = baseStatus;
			this._path = path;
		}
		
		public long getLen() {
			if (!_baseStatus.isFile())
				return _baseStatus.getLen();
			try {
				PathTranslator ptran = new PathTranslator(_path);
				if (!isRaidedFile(ptran)) {
					FileStatus originStatus = baseFS.getFileStatus(ptran.getBaseOriginPath());
					return originStatus.getLen();
				}
			} catch (IOException e) {
				e.printStackTrace();
				return -1;
			}
			LOG.warn(String.format("getLen(%s) for raided file unimpl", _path));
			return 100;
		}
		
		public boolean isFile() {
			return _baseStatus.isFile();
		}

		public boolean isDirectory() {
			return _baseStatus.isDirectory();
		}
		
		@Deprecated
		public boolean isDir() {
			return _baseStatus.isDirectory();
		}
		
		/* doesn't support symlink */
		public boolean isSymlink() {
			return false;
		}
		
		public long getBlockSize() {
			return _baseStatus.getBlockSize();
		}

		/* only 1 replication */
		public short getReplication() {
			return 1;
		}

		public long getModificationTime() {
			return _baseStatus.getModificationTime();
		}

		public long getAccessTime() {
			return _baseStatus.getAccessTime();
		}

		public FsPermission getPermission() {
			return _baseStatus.getPermission();
		}

		public String getOwner() {
			return _baseStatus.getOwner();
		}

		public String getGroup() {
			return _baseStatus.getGroup();
		}
		
		public Path getPath() {
			return _path;
		}
		
		public void setPath(final Path p) {
			_path = p;
		}

		public Path getSymlink() throws IOException {
			return null;
		}
		
		public Path setSymlink() throws IOException {
			return null;
		}
		
		public void readFields(DataInput in) throws IOException {
			_baseStatus.readFields(in);
			this._path = _baseStatus.getPath();
			this._length = _baseStatus.getLen();
		}
	}

	/////////////
	// Layout, dir, fileStatus stuff
	/////////////
	
	public NKFileSystem() throws IOException {
		super();
		workingDir = new Path("/");
		LOG.debug("nkfs object created");
	}
	
	private void createBaseFSLayout() throws IOException {
		LOG.debug("createBaseFSLayout");
		baseFS.mkdirs(getBaseMetaDir());
		baseFS.mkdirs(getBaseShadowDir());
		baseFS.mkdirs(getBaseOriginDir());
		baseFS.mkdirs(getBaseParityDir());
		LOG.debug("layout OK");
	}
	
	@Override
	public void initialize(URI uri, Configuration conf) throws IOException {
		LOG.debug("NKFS is initialized: uri = " + uri.toString());
		this.conf = conf;
		if (this.conf == null)
			this.conf = new Configuration();
		/* Load configuration */
		baseFSURI = conf.get("nkfs.baseFSURI", "hdfs:///");
		basedir = new Path(conf.get("nkfs.basedir", "/"));
		N = conf.getInt("nkfs.parity.n", 5);
		K = conf.getInt("nkfs.parity.k", 3);
		int pieceSizeMB = conf.getInt("nkfs.piece.sizeMB", 64);
		pieceSize = pieceSizeMB * 1024 * 1024;
		
		LOG.debug("Configuration of NKFS: ");
		LOG.debug("\tbaseFSURI: " + baseFSURI);
		LOG.debug("\tbasedir: " + basedir.toString());
		LOG.debug("\tN: " + Integer.toString(N));
		LOG.debug("\tK: " + Integer.toString(K));
		LOG.debug("\tpieceSize: " + Integer.toString(pieceSize));
		
		this.baseFS = FileSystem.get(URI.create(baseFSURI), this.conf);
		/* baseFS should been initialized during get method */
		LOG.debug("baseFS of NKFS is " + baseFS.getClass());
		createBaseFSLayout();
	}

	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		LOG.debug("getFileStatus(" + f + ")");
		checkPath(f);
		/* check shadow dir */
		Path baseShadowPath = new PathTranslator(f).getBaseShadowPath();
		LOG.debug("getFileStatus: check baseFS: " + baseShadowPath);
		FileStatus baseStatus = baseFS.getFileStatus(baseShadowPath);
		LOG.debug("getFileStatus: baseFS returns: " + baseStatus);
		return new NKFileStatus(baseStatus, makeQualified(f));
	}

	@Override
	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		LOG.debug("mkdirs: " + f);
		PathTranslator ptran = new PathTranslator(f);
		try {
			LOG.debug("mkdir in baseFS/origin: " + ptran.getBaseOriginPath());
			baseFS.mkdirs(ptran.getBaseOriginPath(), permission);
			LOG.debug("mkdir in baseFS/metadata: " + ptran.getBaseMetaDataPath());;
			baseFS.mkdirs(ptran.getBaseMetaDataPath(), permission);
			LOG.debug("mkdir in baseFS/parity: " + ptran.getBaseParityPath());;
			baseFS.mkdirs(ptran.getBaseParityPath(), permission);
			LOG.debug("mkdir in baseFS/shadow: " + ptran.getBaseShadowPath());
			baseFS.mkdirs(ptran.getBaseShadowPath(), permission);
			return true;
		} catch (IOException e) {
			baseFS.delete(ptran.getBaseOriginPath(), true);
			baseFS.delete(ptran.getBaseMetaDataPath(), true);
			baseFS.delete(ptran.getBaseParityPath(), true);
			baseFS.delete(ptran.getBaseOriginPath(), true);
			e.printStackTrace();
			throw e;
		}
	}

	@Override
	public FileStatus[] listStatus(Path f) throws FileNotFoundException,
			IOException {
		
		LOG.info(String.format("listStatus(%s)", f.toString()));
		PathTranslator ptrans = new PathTranslator(f);
		
		FileStatus[] baseStatus = baseFS.listStatus(ptrans.getBaseShadowPath());
		FileStatus[] resStatus = new FileStatus[baseStatus.length];
		
		int i = 0;
		for (FileStatus stat : baseStatus) {
			FileStatus r = new NKFileStatus(stat, convertFromBaseShadow(stat.getPath()));
			resStatus[i++] = r;
			LOG.info(r.getPath());
		}
		return resStatus;
	}
	
	private boolean isRaidedFile(PathTranslator ptran) throws IOException {
		FileStatus s_shadow = baseFS.getFileStatus(ptran.getBaseShadowPath());
		if (!s_shadow.isFile())
			return false;
		if (baseFS.exists(ptran.getBaseOriginPath()))
			return false;
		if (!baseFS.exists(ptran.getBaseMetaDataPath()))
			return true;
		throw new IOException("nkfs corrupted: check path " + ptran.getBaseShadowPath());
	}
	
	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		LOG.debug("rename: " + src + " to " + dst);
		PathTranslator ptran_src = new PathTranslator(src);
		PathTranslator ptran_dst = new PathTranslator(dst);
		
		FileStatus s_shadow = baseFS.getFileStatus(ptran_src.getBaseShadowPath());
		if (s_shadow.isDirectory()) {
			/* simple situation */
			baseFS.rename(ptran_src.getBaseMetaDataPath(), ptran_dst.getBaseMetaDataPath());
			baseFS.rename(ptran_src.getBaseOriginPath(), ptran_dst.getBaseOriginPath());
			baseFS.rename(ptran_src.getBaseParityPath(), ptran_dst.getBaseParityPath());
			baseFS.rename(ptran_src.getBaseShadowPath(), ptran_dst.getBaseShadowPath());
			return true;
		}
		
		/* rename a file */
		/* first: check shadow path */
		assert s_shadow.isFile() : s_shadow.getPath().toString() + " should be a regular file";
		if (!isRaidedFile(ptran_src)) {
			baseFS.rename(ptran_src.getBaseOriginPath(), ptran_dst.getBaseOriginPath());
			baseFS.rename(ptran_src.getBaseShadowPath(), ptran_dst.getBaseShadowPath());
			return true;
		}
		
		assert (baseFS.exists(ptran_src.getBaseParityPath()));
		assert (baseFS.exists(ptran_src.getBaseMetaDataPath()));
		
		baseFS.rename(ptran_src.getBaseMetaDataPath(), ptran_dst.getBaseMetaDataPath());
		baseFS.rename(ptran_src.getBaseOriginPath(), ptran_dst.getBaseOriginPath());
		baseFS.rename(ptran_src.getBaseShadowPath(), ptran_dst.getBaseShadowPath());
		return true;
	}
	
	/////////////////////////////
	// File IO stuff
	/////////////////////////////

	@Override
	/* create for write */
	public FSDataOutputStream create(Path f, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		LOG.debug("creating path " + f);
		PathTranslator ptran = new PathTranslator(f);
		try {
			/* don't make metadata dir: indicate that the file still in origin */
			/* create an entry in shadow */
			LOG.debug("creating shadow: " + ptran.getBaseShadowPath());
			
			/* if there is a file in shadow base, but not in original base,
			 * then overwrite is forbidden 
			 * */
			if (baseFS.exists(ptran.getBaseShadowPath())) {
				if (!baseFS.exists(ptran.getBaseOriginPath())) {
					throw new IOException("Overwrite a raided file is not allow");
				}
			}
			
			baseFS.create(ptran.getBaseShadowPath(),
					permission, overwrite, bufferSize, replication, blockSize,
					progress).close();
			/* create origin entry */
			LOG.debug("creating origin: " + ptran.getBaseOriginPath());
			FSDataOutputStream os = baseFS.create(ptran.getBaseOriginPath(),
					permission, overwrite, bufferSize, replication, blockSize,
					progress);
			return os;
		} catch (IOException e) {
			LOG.warn("file creation failed");
			baseFS.delete(ptran.getBaseShadowPath(), false);
			baseFS.delete(ptran.getBaseOriginPath(), false);
			e.printStackTrace();
			throw e;
		}
	}

	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {
		LOG.debug("deleting path " + f);
		PathTranslator ptran = new PathTranslator(f);
		FileStatus s_shadow = baseFS.getFileStatus(ptran.getBaseShadowPath());
		
		if (s_shadow.isDirectory()) {
			baseFS.delete(ptran.getBaseMetaDataPath(), true);
			baseFS.delete(ptran.getBaseOriginPath(), true);
			baseFS.delete(ptran.getBaseParityPath(), true);
			baseFS.delete(ptran.getBaseShadowPath(), true);
			return true;
		}
		
		if (!isRaidedFile(ptran)) {
			baseFS.delete(ptran.getBaseOriginPath(), false);
			baseFS.delete(ptran.getBaseShadowPath(), false);
			return true;
		}
		
		baseFS.delete(ptran.getBaseMetaDataPath(), true);
		baseFS.delete(ptran.getBaseParityPath(), true);
		baseFS.delete(ptran.getBaseShadowPath(), true);
		
		return true;
	}


	@Override
	public FSDataOutputStream append(Path f, int bufferSize,
			Progressable progress) throws IOException {
		LOG.debug("appending path " + f);
		PathTranslator ptran = new PathTranslator(f);
		if (!isRaidedFile(ptran)) {
			return baseFS.append(ptran.getBaseOriginPath(),
					bufferSize, progress);
		}
		LOG.error("try to append to raided file " + f);
		throw new IOException("doesn't allow append to raided file");
	}

	
	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		PathTranslator ptrans = new PathTranslator(f);
		return null;
	}

	public static void main(String[] args) {
		URI uri_base = URI.create("http://www.google.com/path1");
		URI uri = URI.create("http://www.google.com/path1/path2");
		System.out.println(uri_base.relativize(uri));
	}

}
