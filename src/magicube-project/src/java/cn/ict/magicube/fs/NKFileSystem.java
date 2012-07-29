package cn.ict.magicube.fs;

import java.io.DataInput;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern; 
import java.util.regex.Matcher; 

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.shell.PathExceptions.PathIOException;
import org.apache.hadoop.fs.GlobFilter;
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
 *                  |- /part-<offset>-<length>
 *                       origin
 *                       parity_0001
 *                       parity_0002
 *                       ...
 *                       parity_n
 *                     /part-<offset>-<length>
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

	static Path resolvePath(Path p, String r) {
		/* don't "makeQualified". 
		 * -- OR --
		 * plus the final '/' after qualified URI
		 *  */
		return new Path(p.toUri().resolve(r));
	}
	
	static Path resolvePathToDir(Path p, String r) {
		/* don't "makeQualified". 
		 * -- OR --
		 * plus the final '/' after qualified URI
		 *  */
		return new Path(p.toUri().resolve(r + Path.SEPARATOR));
	}
	
	public class PathTranslator {
		private String _abs;
		public PathTranslator(Path path) {
			Path qp = makeQualified(path);
			String strpath = qp.toUri().getPath();
			/* remove leading '/' */
			int i = 0;
			for (i = 0; i < strpath.length(); i++) {
				if (strpath.charAt(i) != Path.SEPARATOR_CHAR)
					break;
			}
			_abs = strpath.substring(i);
		}
		
		/*
		private String getDigest() {
			if (_digest != null)
				return _digest;
			_digest = MD5Hash.digest(_abs).toString();
			return _digest;
		}
		*/
		
		
		public Path getBaseOriginPath() {
			return resolvePath(getBaseOriginDir(), _abs);
		}
		
		public Path getBaseShadowPath() {
			return resolvePath(getBaseShadowDir(), _abs);
		}
		
		public Path getBaseMetaDataDirPath() {
			return resolvePathToDir(getBaseMetaDir(), _abs);

		}
		
		public Path getBaseParityDirPath() {
			return resolvePathToDir(getBaseParityDir(), _abs);
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
	private static int N = -1;
	public int getN() {
		if (N > 0)
			return N;
		N = conf.getInt("nkfs.parity.n", 5);
		return N;
	}
	/* nkfs.parity.k, default: 3 */
	private static int K = -1;
	public int getK() {
		if (K > 0)
			return K;
		K = conf.getInt("nkfs.parity.k", 3);
		return K;
	}

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
		URI base_root = URI.create(baseRoot.toUri().getPath());
		URI u = URI.create(p.toUri().getPath());
		String rel = Path.SEPARATOR + base_root.relativize(u).getPath();
		LOG.debug(String.format("convertFromBasePath(%s, %s) = %s",
				baseRoot, p, rel));
		return new Path(rel);
	}
	
	public Path convertFromBaseShadow(Path p) {
		return convertFromBasePath(getBaseShadowDir(), p);
	}
	
	public Path convertFromBaseOrigin(Path p) {
		return convertFromBasePath(getBaseOriginDir(), p);
	}
	
	public Path convertFromBaseMetadata(Path p) {
		return convertFromBasePath(getBaseMetaDir(), p);
	}

	public static Path convertToDir(Path p) {
		return new Path(
			URI.create(
				p.toUri().toString() + Path.SEPARATOR
			)
		);
	}
	
	////////////////
	// NKFS specific stuffs
	////////////////
	FileSystem getBaseFileSystem() {
		return baseFS;
	}
	
	private static FsAction removeWrite(FsAction old) {
		switch (old) {
		case WRITE:
			return FsAction.NONE;
		case WRITE_EXECUTE:
			return FsAction.EXECUTE;
		case READ_WRITE:
			return FsAction.READ;
		case ALL:
			return FsAction.READ_EXECUTE;
		default:
			return old;
		}
	}
	
	private static FsAction addWrite(FsAction old) {
		switch (old) {
		case NONE:
			return FsAction.WRITE;
		case READ:
			return FsAction.READ_WRITE;
		case EXECUTE:
			return FsAction.WRITE_EXECUTE;
		case READ_EXECUTE:
			return FsAction.ALL;
		default:
			return old;
		}
	}
	
	private static FsPermission addWritePermission(FsPermission old) {
		FsAction u = old.getUserAction();
		FsAction g = old.getGroupAction();
		FsAction o = old.getOtherAction();
		u = addWrite(u);
		g = addWrite(g);
		o = addWrite(o);
		return new FsPermission(u, g, o, old.getStickyBit());
	}
	
	private static FsPermission removeWritePermission(FsPermission old) {
		FsAction u = old.getUserAction();
		FsAction g = old.getGroupAction();
		FsAction o = old.getOtherAction();
		u = removeWrite(u);
		g = removeWrite(g);
		o = removeWrite(o);
		return new FsPermission(u, g, o, old.getStickyBit());
	}
	
	void prepareTransformOriginPath(Path originPath) throws IOException {
		/* add write permission for the file */
		LOG.debug("prepare transform origin file " + originPath.toString());
		FileStatus stat = baseFS.getFileStatus(originPath);
		baseFS.setPermission(originPath,
				addWritePermission(stat.getPermission()));
		stat = baseFS.getFileStatus(originPath);
		
		/* create Metadata dir */
		Path topPath = convertFromBaseOrigin(originPath);
		LOG.debug("topPath = " + topPath.toUri());
		PathTranslator ptran = new PathTranslator(topPath);
		Path metadataDir = ptran.getBaseMetaDataDirPath();
		LOG.debug("metadataDir = " + metadataDir.toUri());
		baseFS.mkdirs(metadataDir, stat.getPermission());
		
		/* create length file */
		String length_fn = "length-" + stat.getLen();
		Path lengthFile = new Path(metadataDir.toUri().resolve(length_fn));
		LOG.debug("creating length file: " + lengthFile.toUri());
		baseFS.create(lengthFile).close();
	}
	
	void finishTransformOriginPath(Path originPath) throws IOException {
		/* remove shadow's write permission  */
		LOG.debug("finishing transform for " + originPath);
		
		FileStatus stat = baseFS.getFileStatus(originPath);
		FsPermission perm = stat.getPermission();
		perm = removeWritePermission(perm);
		
		Path topPath = convertFromBaseOrigin(originPath);
		PathTranslator ptran = new PathTranslator(topPath);
		baseFS.setPermission(ptran.getBaseShadowPath(), perm);
		baseFS.delete(originPath, false);
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
	
	@Override
	public void setPermission(Path p, FsPermission perm) throws IOException {
		PathTranslator ptran = new PathTranslator(p);
		baseFS.setPermission(ptran.getBaseShadowPath(), perm);
		return;
	}
	
	class NKFileStatus extends FileStatus {
		private FileStatus _baseStatus;
		private Path _path;
		private long _length = -1;
		public NKFileStatus(FileStatus baseStatus, Path path) {
			this._baseStatus = baseStatus;
			this._path = path;
		}
		
		public long getLen() {
			if (!_baseStatus.isFile())
				return _baseStatus.getLen();
			if (_length > 0)
				return _length;
			try {
				PathTranslator ptran = new PathTranslator(_path);
				if (!isRaidedFile(ptran)) {
					FileStatus originStatus = baseFS.getFileStatus(ptran.getBaseOriginPath());
					return originStatus.getLen();
				}
				
				/* check metadata */
				//LOG.warn(String.format("getLen(%s) for raided file unimpl", _path));
				Path metaDataPath = ptran.getBaseMetaDataDirPath();
				FileStatus[] stats = baseFS.listStatus(metaDataPath, new GlobFilter("length-*"));
				//LOG.warn(stats[0].getPath().getName());
				String fn = stats[0].getPath().getName();
				String length_str = fn.replaceFirst("length-", "");
				_length = Long.parseLong(length_str);
				return _length;
			} catch (IOException e) {
				e.printStackTrace();
				return -1;
			}
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
		int pieceSizeMB = conf.getInt("nkfs.piece.sizeMB", 64);
		pieceSize = pieceSizeMB * 1024 * 1024;
		
		LOG.debug("Configuration of NKFS: ");
		LOG.debug("\tbaseFSURI: " + baseFSURI);
		LOG.debug("\tbasedir: " + basedir.toString());
		LOG.debug("\tN: " + getN());
		LOG.debug("\tK: " + getK());
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
			LOG.debug("mkdir in baseFS/metadata: " + ptran.getBaseMetaDataDirPath());;
			baseFS.mkdirs(ptran.getBaseMetaDataDirPath(), permission);
			LOG.debug("mkdir in baseFS/parity: " + ptran.getBaseParityDirPath());;
			baseFS.mkdirs(ptran.getBaseParityDirPath(), permission);
			LOG.debug("mkdir in baseFS/shadow: " + ptran.getBaseShadowPath());
			baseFS.mkdirs(ptran.getBaseShadowPath(), permission);
			return true;
		} catch (IOException e) {
			baseFS.delete(ptran.getBaseOriginPath(), true);
			baseFS.delete(ptran.getBaseMetaDataDirPath(), true);
			baseFS.delete(ptran.getBaseParityDirPath(), true);
			baseFS.delete(ptran.getBaseOriginPath(), true);
			e.printStackTrace();
			throw e;
		}
	}

	@Override
	public FileStatus[] listStatus(Path f) throws FileNotFoundException,
			IOException {
		
		LOG.debug(String.format("listStatus(%s)", f.toString()));
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
		if (baseFS.exists(ptran.getBaseMetaDataDirPath()))
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
			baseFS.rename(ptran_src.getBaseMetaDataDirPath(), ptran_dst.getBaseMetaDataDirPath());
			baseFS.rename(ptran_src.getBaseOriginPath(), ptran_dst.getBaseOriginPath());
			baseFS.rename(ptran_src.getBaseParityDirPath(), ptran_dst.getBaseParityDirPath());
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
		
		assert (baseFS.exists(ptran_src.getBaseParityDirPath()));
		assert (baseFS.exists(ptran_src.getBaseMetaDataDirPath()));
		
		baseFS.rename(ptran_src.getBaseMetaDataDirPath(), ptran_dst.getBaseMetaDataDirPath());
		baseFS.rename(ptran_src.getBaseOriginPath(), ptran_dst.getBaseOriginPath());
		baseFS.rename(ptran_src.getBaseShadowPath(), ptran_dst.getBaseShadowPath());
		return true;
	}
	
	//@Override
	
	
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
			baseFS.delete(ptran.getBaseMetaDataDirPath(), true);
			baseFS.delete(ptran.getBaseOriginPath(), true);
			baseFS.delete(ptran.getBaseParityDirPath(), true);
			baseFS.delete(ptran.getBaseShadowPath(), true);
			return true;
		}
		
		if (!isRaidedFile(ptran)) {
			baseFS.delete(ptran.getBaseOriginPath(), false);
			baseFS.delete(ptran.getBaseShadowPath(), false);
			return true;
		}
		
		baseFS.delete(ptran.getBaseMetaDataDirPath(), true);
		baseFS.delete(ptran.getBaseParityDirPath(), true);
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

	
	private static class NKFSInputStream extends InputStream implements Seekable, PositionedReadable {
		private static class PartInfo implements Comparable<PartInfo> {
			final Path _partDir;
			final Path _partOriginFile;
			final Path _topFilePath;
			final long _offset;
			final long _length;
			
			PartInfo(Path originFilePath, Path partDir) throws IOException {
				String partName = partDir.getName();
				Pattern pattern = Pattern.compile("part-(\\d+)-(\\d+)");
				Matcher m = pattern.matcher(partName);
				
				if (!m.find()) {
					throw new IOException(String.format(
							"unable to explain part dir %s",
							partDir));
				}
				_offset = Long.parseLong(m.group(1));
				_length = Long.parseLong(m.group(2));
				_partDir = convertToDir(partDir);
				_topFilePath =  originFilePath;
				_partOriginFile = new Path(_partDir.toUri().resolve("origin"));
			}
			
			@Override
			public String toString() {
				return String.format("%s (%s): %d -- +%d",
						_topFilePath, _partDir, _offset, _length);
			}

			@Override
			public int compareTo(PartInfo o) {
				PartInfo other = (PartInfo)o;
				if (other._offset - this._offset == 0)
					return 0;
				if (other._offset - this._offset > 0)
					return -1;
				return 1;
			}
		};
		
		private final FileSystem _baseFS;
		private final int _bufferSize;
		private final Path _topPath;

		private PartInfo[] parts;
		private long curPos;
		private int curPartIdx;
		private FSDataInputStream curIn;
		
		private static int checkPos(PartInfo p, long pos) {
			if (p._offset > pos)
				return -1;
			if (p._offset + p._length <= pos)
				return 1;
			return 0;
		}
		
		public static class SeekOOBException extends IOException {
			private static final long serialVersionUID = 4539327049741018159L;
			SeekOOBException(String str) {
				super(str);
			}
		}
		
		private int findPart(long pos, int idx) throws IOException {
			if ((idx < 0) || (idx >= parts.length))
				throw new SeekOOBException(String.format("search file %s for %d failed",
						_topPath.toUri(), pos));
			int dir = checkPos(parts[idx], pos);
			if (dir == 0)
				return idx;
			if (dir > 0)
				return findPart(pos, idx + 1);
			return findPart(pos, idx - 1);
		}
		
		private NKFSInputStream(Path topPath, FileStatus[] partsDirs,
				int bufferSize, FileSystem baseFS) throws IOException {
			List<PartInfo> l = new LinkedList<PartInfo>();
			for (FileStatus partDirStat : partsDirs) {
				l.add(new PartInfo(topPath, partDirStat.getPath()));
			}
			
			parts = new PartInfo[l.size()];
			java.util.Collections.sort(l);
			l.toArray(parts);
			curPos = 0;
			curPartIdx = 0;
			_baseFS = baseFS;
			_bufferSize = bufferSize;
			_topPath = topPath;
			curIn = _baseFS.open(parts[curPartIdx]._partOriginFile, bufferSize);
		}
		
		@Override
		public int read(long position, byte[] buffer, int offset, int length)
				throws IOException {
			
			/* check whether current ins can fulfill this request */
			PartInfo curPart = parts[curPartIdx];
			long bytesLeft = curPart._length - (position - curPart._offset);
			if ((curPart._offset <= position) && (bytesLeft > 0)) {
				// bytesLeft == 0 is a special case
				// curIn can fulfill this request
				long bytesRead = Math.min(bytesLeft, (long)length); 
				return curIn.read(position - curPart._offset, buffer, offset, (int)bytesRead);
			}
			
			// find specific part, open it, read then close
			int newIdx = -1;
			try {
				newIdx = findPart(position, curPartIdx);
			} catch (SeekOOBException e) {
				return -1;
			} 
			PartInfo newPart = parts[newIdx];
			FSDataInputStream targetIn = _baseFS.open(newPart._partOriginFile, _bufferSize);
			long pos = position - newPart._offset;
			bytesLeft = newPart._length - (position - newPart._offset);
			long bytesRead = Math.min(bytesLeft, (long)length);
			int r = targetIn.read(pos, buffer, offset, (int)bytesRead);
			targetIn.close();
			return r;
		}
				
		@Override
	    public int read(byte b[], int off, int len) throws IOException {
			int r = curIn.read(b, off, len);
			if (r > 0) {
				try {
					_seek(curPos + r);
				} catch (SeekOOBException e) {
				}
			}
			return r;
			///////////// slow
			/*
			int r = read(curPos, b, off, len);
			if (r > 0) {
				try {
					_seek(curPos + r);
				} catch (SeekOOBException e) {
					
				}
			}
			return r;
			*/
	    }

		@Override
		public void readFully(long position, byte[] buffer, int offset,
				int length) throws IOException {
			int l = 0;
			int left = length;
			while (l < length) {
				position += l;
				offset += l;
				left -= l;
				l += read(position, buffer, offset, length);
			}
		}
		@Override
		public void readFully(long position, byte[] buffer) throws IOException {
			readFully(position, buffer, 0, buffer.length);
		}
		
		private void _seek(long pos) throws IOException {
			curPos = pos;
			PartInfo curPart = parts[curPartIdx];
			int dir = 0;
			if (pos < curPart._offset)
				dir = -1;
			if (pos >= curPart._offset + curPart._length)
				dir = 1;
			if (dir == 0)
				return;
			try {
				curPartIdx = findPart(pos, curPartIdx + dir);
			} catch (SeekOOBException e) {
				PartInfo lastPart = parts[parts.length - 1];
				curPos = lastPart._offset + lastPart._length;
				throw e;
			}
			curIn.close();
			curIn = _baseFS.open(parts[curPartIdx]._partOriginFile, _bufferSize);
		}

		@Override
		public void close() throws IOException {
			super.close();
			curIn.close();
		}
		
		@Override
		public void seek(long pos) throws IOException {
			_seek(pos);
		}
		
		
		@Override
		public long getPos() throws IOException {
			return curPos;
		}
		
		@Override
		public boolean seekToNewSource(long targetPos) throws IOException {
			return false;
		}
		@Override
		public int read() throws IOException {
			byte[] buf = new byte[1];
			read(buf, 0, 1);
			return (int)(buf[0]) & 0xFF;
		}
		
	}
	
	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		PathTranslator ptran = new PathTranslator(f);
		LOG.debug("open file " + f.toString());
		if (baseFS.exists(ptran.getBaseOriginPath())) {
			LOG.debug(f.toString() + " is unraided");
			return baseFS.open(ptran.getBaseOriginPath(), bufferSize);
		}
		
		/* check existence of shadow */
		if (!baseFS.exists(ptran.getBaseShadowPath())) {
			throw new IOException("file " +
					f.toString() + " not exist");
		}
		
		/* check parity path */
		FileStatus[] parts = baseFS.listStatus(ptran.getBaseParityDirPath());
		return new FSDataInputStream(new NKFSInputStream(f, parts, bufferSize, baseFS));
	}

	public static void main(String[] args) {
		URI uri_base = URI.create("http://www.google.com/path1");
		URI uri = URI.create("http://www.google.com/path1/path2/");
		System.out.println(uri_base.relativize(uri));
		System.out.println(uri.resolve("test"));
	}

}
