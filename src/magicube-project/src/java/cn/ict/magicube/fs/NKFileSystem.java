package cn.ict.magicube.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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

import cn.ict.magicube.spikes.FakeFileSystem2;

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
 *   |- /parities
 *        |- /<md5 of file path>
 *             |- /pieces
 *                  |- /part_0001
 *                       parity_0001
 *                       parity_0002
 *                       ...
 *                       parity_n
 *                     /part_0002
 *                       parity_0001
 *                       parity_0002
 *                       ...
 *
 *  configurations:
 *  
 *  nkfs.piece.sizeMB: size of a part of a file
 *  nkfs.parity.n
 *  nkfs.parity.k
 *  nkfs.basedir
 */
public class NKFileSystem extends FileSystem {
	static final URI NAME = URI.create("nkfs:///");
	static final Log LOG = LogFactory.getLog(NKFileSystem.class);
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
	
	private Path getBasePath(String p) {
		return new Path(basedir + Path.SEPARATOR + p);
	}
	
	private Path getBaseMetaDir() {
		return getBasePath("metadata");
	}
	
	private Path getBaseOriginDir() {
		return getBasePath("origin");
	}

	private Path getBaseShadowDir() {
		return getBasePath("shadow");
	}
	
	private Path getBaseParityDir() {
		return getBasePath("parity");
	}

	////////////////
	// trivial stuff
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
	
	static class NKFileStatus extends FileStatus {
		
	}

	/////////////
	// key stuff
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
		Path shadowP = getBaseShadowDir();
		shadowP.suffix("121212");
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
	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

}
