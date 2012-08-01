package cn.ict.magicube.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsAction;
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

	public PartInfo[] getParts(NKPathTranslator ptran) throws IOException {
		FileStatus[] stats = _baseFS.listStatus(ptran.getParityDirPath());
		List<PartInfo> partsList = new LinkedList<PartInfo>();
		for (FileStatus stat : stats) {
			partsList.add(new PartInfo(
					_baseFS.makeQualified(stat.getPath())
					));
		}
		java.util.Collections.sort(partsList);
		PartInfo[] parts = new PartInfo[partsList.size()];
		partsList.toArray(parts);
		return parts;
	}
	
	public class NKFSInputStream extends InputStream implements Seekable, PositionedReadable {
		
		final PartInfo[] _parts;
		final NKPathTranslator _ptran;
		
		long _curPos;
		int _curPartIdx;
		final int _bufferSize;
		FSDataInputStream _curIn;
		
		private PartInfo getCurPart() {
			return _parts[_curPartIdx];
		}
		
		public NKFSInputStream(NKPathTranslator ptran, int bufferSize) throws IOException {
			_ptran = ptran;
			_bufferSize = bufferSize;
			_parts = getParts(ptran);
			_curPos = 0;
			_curPartIdx = 0;
			_curIn = _baseFS.open(getCurPart().getOriginFile(NKFileSystem.this), _bufferSize);
		}
		
		private int checkPos(PartInfo p, long pos) {
			if (p.offset > pos)
				return -1;
			if (p.offset + p.length <= pos)
				return 1;
			return 0;
		}
		
		private int findPart(long pos, int idx) throws IOException {
			// shouldn't happen
			if (idx < 0)
				return -1;
			if (idx >= _parts.length)
				return -2;
			int dir = checkPos(_parts[idx], pos);
			if (dir == 0)
				return idx;
			if (dir > 0)
				return findPart(pos, idx + 1);
			return findPart(pos, idx - 1);
		}

		/*
		 * retval of _seek:
		 * 0: normal
		 * -1: seek failed
		 */
		private int _seek(long pos) throws IOException {
			_curPos = pos;
			int dir = checkPos(getCurPart(), pos);
			if (dir == 0)
				return 0;
			int newIdx = findPart(pos, _curPartIdx + dir);
			if (newIdx >= 0) {
				_curPartIdx = newIdx;
				_curIn.close();
				_curIn = _baseFS.open(
						getCurPart().getOriginFile(NKFileSystem.this),
						_bufferSize
						);
				return 0;
			}
			return -1;
		}
		
		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			int r = _curIn.read(b, off, len);
			if (r > 0) {
				_seek(_curPos + r);
			}
			return r;
		}
		
		@Override
		public int read(long position, byte[] buffer, int offset, int length)
				throws IOException {
			// check whether current ins can fulfill this request
			PartInfo curPart = getCurPart();
			long bytesLeft = curPart.length - (position - curPart.offset);
			if ((curPart.offset <= position) && (bytesLeft > 0)) {
				// bytesLeft == 0 is a special case.
				// curIn can fulfill this request.
				long bytesRead = Math.min(bytesLeft, (long)length); 
				return _curIn.read(position - curPart.offset, buffer, offset, (int)bytesRead);
			}
			
			// curIn cannot fulfill this request
			int newIdx = findPart(position, _curPartIdx);
			if (newIdx < 0)
				return -1;
			PartInfo newPart = _parts[newIdx];
			FSDataInputStream targetIn = _baseFS.open(
					newPart.getOriginFile(NKFileSystem.this), _bufferSize);
			long pos = position - newPart.offset;
			bytesLeft = newPart.length - (position - newPart.offset);
			long bytesRead = Math.min(bytesLeft, (long)length);
			int r = targetIn.read(pos, buffer, offset, (int)bytesRead);
			targetIn.close();
			return r;
		}

		@Override
		public void readFully(long position, byte[] buffer, int offset,
				int length) throws IOException {
			int l = 0;
			int left = length;
			while (l < left) {
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

		@Override
		public void seek(long pos) throws IOException {
			int seekres = _seek(pos);
			if (seekres < 0)
				throw new IOException(String.format(
						"Seek failed for file %s: seek %d",
						_ptran.getTopFSPath().toString(), pos));
		}

		@Override
		public long getPos() throws IOException {
			return _curPos;
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

	public void prepareTransform(NKPathTranslator ptran) throws IOException {
		/* add write permission for the file */
		Path originPath = ptran.getOriginPath();
		LOG.debug("prepare transform " + ptran.getTopFSPath().toString());
		FileStatus stat = _baseFS.getFileStatus(originPath);
		_baseFS.setPermission(originPath,
				addWritePermission(stat.getPermission()));
		stat = _baseFS.getFileStatus(originPath);
		
		/* create Metadata dir */
		Path metadataDir = ptran.getMetadataDirPath();
		LOG.debug("metadataDir = " + metadataDir.toUri());
		_baseFS.mkdirs(metadataDir, stat.getPermission());
		
		/* create length file */
		String length_fn = "length-" + stat.getLen();
		Path lengthFile = new Path(metadataDir.toUri().resolve(length_fn));
		LOG.debug("creating length file: " + lengthFile.toUri());
		_baseFS.create(lengthFile).close();
	}
	
	public void finishTransform(NKPathTranslator ptran) throws IOException {
		Path originPath = ptran.getOriginPath();
		
		/* remove shadow's write permission  */
		LOG.debug("finishing transform for " + originPath);
		FileStatus stat = _baseFS.getFileStatus(originPath);
		FsPermission perm = stat.getPermission();
		perm = removeWritePermission(perm);
		_baseFS.setPermission(ptran.getShadowPath(), perm);
		_baseFS.delete(originPath, false);
	}

	
	public FileSystem getBaseFS() {
		return _baseFS;
	}

	////////////////
	// trivial stuffs
	////////////////
	
	@Override
	public short getDefaultReplication() {
		return _baseFS.getDefaultReplication();
	}
	public FsStatus getStatus(Path p) throws IOException {
		NKPathTranslator ptran = new NKPathTranslator(p);
		return _baseFS.getStatus(ptran.getShadowPath());
	}
	public boolean setReplication(Path src, short replication)
			throws IOException {
		NKPathTranslator ptran = new NKPathTranslator(src);
		if (!ptran.isRaidedFile())
			return _baseFS.setReplication(ptran.getOriginPath(), replication);
		return true;
	}
	public void setTimes(Path p, long mtime, long atime) throws IOException {
		NKPathTranslator ptran = new NKPathTranslator(p);
		_baseFS.setTimes(ptran.getShadowPath(), mtime, atime);
	}
	public void setOwner(Path p, String username, String groupname) throws IOException {
		NKPathTranslator ptran = new NKPathTranslator(p);
		_baseFS.setOwner(ptran.getShadowPath(), username, groupname);
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
		URI baseFSURI = URI.create(conf.get("nkfs.baseFSURI", "hdfs:///"));
		this._baseFS = FileSystem.get(baseFSURI, this.getConf());
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
			LOG.debug("target is dir: " + f);
			_baseFS.delete(ptran.getMetadataDirPath(), true);
			_baseFS.delete(ptran.getOriginPath(), true);
			_baseFS.delete(ptran.getParityDirPath(), true);
			_baseFS.delete(ptran.getShadowPath(), true);
			return true;
		}
		
		if (!ptran.isRaidedFile()) {
			LOG.debug("target is unraided file: " + f);
			_baseFS.delete(ptran.getOriginPath(), true);
			_baseFS.delete(ptran.getShadowPath(), true);
			return true;
		}
		LOG.debug("target raided file: " + f);
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
		LOG.debug("replication=" + replication);
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
		NKPathTranslator ptran = new NKPathTranslator(f);
		if (_baseFS.exists(ptran.getOriginPath())) {
			LOG.debug(f.toString() + " is unraided");
			return _baseFS.open(ptran.getOriginPath(), bufferSize);
		}
		
		if (!_baseFS.exists(ptran.getShadowPath()))
			throw new FileNotFoundException("Shadow of " + f.toString() + " is not exist");
		
		return new FSDataInputStream(new NKFSInputStream(ptran, bufferSize));
	}

	////////////////
	// Other stuff
	////////////////
	
	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file,
			long start, long len) throws IOException {
		if (file == null) {
			return null;
		}
		if (start < 0 || len < 0) {
			throw new IllegalArgumentException("Invalid start or len parameter");
		}
		if (file.getLen() < start) {
			return new BlockLocation[0];
		}
		NKPathTranslator ptran = new NKPathTranslator(file.getPath());
		if (!ptran.isRaidedFile())
			return _baseFS.getFileBlockLocations(ptran.getOriginPath(), start, len);

		LOG.debug("getFileBlockLocations for rained file " + file.getPath());
		PartInfo[] parts = getParts(ptran);
		List<BlockLocation> l_bls = new LinkedList<BlockLocation>();
		for (PartInfo p : parts) {
			LOG.debug("check part " + p);
			if (p.offset + p.length < start)
				continue;
			if (start + len <= p.offset)
				continue;
			BlockLocation[] origin_bls = _baseFS.getFileBlockLocations(p.getOriginFile(this),
					0, p.length);
			for (BlockLocation b : origin_bls) {
				long o_offset = b.getOffset();
				b.setOffset(o_offset + p.offset);
				l_bls.add(b);
				LOG.debug("add part " + p);
			}
		}
		BlockLocation[] result = new BlockLocation[l_bls.size()];
		return l_bls.toArray(result);
	}
	
	public static void main(String[] args) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(args[0]), new Configuration());
		fs.initialize(URI.create(args[0]), new Configuration());
		
		long start = Long.parseLong(args[1]);
		long end = Long.parseLong(args[2]);
		BlockLocation[] bls = fs.getFileBlockLocations(new Path(args[0]),
				start, end - start);
		for (BlockLocation bl : bls) {
			System.out.println(bl.toString());
			for (String h : bl.getNames()) {
				System.out.format("\t%s\n", h);
			}
		}
	}
}
