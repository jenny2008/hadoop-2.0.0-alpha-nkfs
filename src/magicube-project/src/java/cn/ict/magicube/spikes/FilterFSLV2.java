package cn.ict.magicube.spikes;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

public class FilterFSLV2 extends FilterFileSystem {
	
	static final private Log LOG = LogFactory.getLog(FilterFSLV2.class);

	
	public FilterFSLV2(FileSystem fs) {
		super(fs);
	}
	
	  /** Called after a new FileSystem instance is constructed.
	   * @param name a uri whose authority section names the host, port, etc.
	   *   for this FileSystem
	   * @param conf the configuration
	   */
	  public void initialize(URI name, Configuration conf) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());
		  LOG.info(String.format("initialize(%s, ??)",
				  name.toString()));
	    super.initialize(name, conf);
	  }

	  /** Returns a URI whose scheme and authority identify this FileSystem.*/
	  public URI getUri() {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());
		  LOG.info("getUri()");
	    return super.getUri();
	  }

	  /**
	   * Returns a qualified URI whose scheme and authority identify this
	   * FileSystem.
	   */
	  @Override
	  protected URI getCanonicalUri() {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());
		  LOG.info("getCanonicalUri()");
	    return super.getCanonicalUri();
	  }
	  
	  /** Make sure that a path specifies a FileSystem. */
	  public Path makeQualified(Path path) {
		  LOG.info(String.format("makeQualified(%s)", path.toString()));
		  Path p = super.makeQualified(path);
		  LOG.info("result: " + p.toString());
		  return p;
	  }
	  
	  ///////////////////////////////////////////////////////////////
	  // FileSystem
	  ///////////////////////////////////////////////////////////////

	  /** Check that a Path belongs to this FileSystem. */
	  protected void checkPath(Path path) {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());
		  LOG.info(String.format("checkPath(%s)", path.toString()));
	    super.checkPath(path);
	  }

	  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
	    long len) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

		  LOG.info(String.format("getFileBlockLocations(??, %ld, %ld)",
				  start, len));
	      return super.getFileBlockLocations(file, start, len);
	  }

	  @Override
	  public Path resolvePath(final Path p) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

		  LOG.info(String.format("resolvePath(%s)",
				  p.toString()));
	    return super.resolvePath(p);
	  }
	  /**
	   * Opens an FSDataInputStream at the indicated Path.
	   * @param f the file name to open
	   * @param bufferSize the size of the buffer to be used.
	   */
	  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

		  LOG.info(String.format("open(%s, %d)",
				  f.toString(), bufferSize));
	    return super.open(f, bufferSize);
	  }

	  /** {@inheritDoc} */
	  public FSDataOutputStream append(Path f, int bufferSize,
	      Progressable progress) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

		  LOG.info(String.format("append(%s, %d, ??)",
				  f.toString(), bufferSize));
		  return super.append(f, bufferSize, progress);
	  }

	  /** {@inheritDoc} */
	  @Override
	  public FSDataOutputStream create(Path f, FsPermission permission,
	      boolean overwrite, int bufferSize, short replication, long blockSize,
	      Progressable progress) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.create(f, permission,
	        overwrite, bufferSize, replication, blockSize, progress);
	  }

	  /**
	   * Set replication for an existing file.
	   * 
	   * @param src file name
	   * @param replication new replication
	   * @throws IOException
	   * @return true if successful;
	   *         false if file does not exist or is a directory
	   */
	  public boolean setReplication(Path src, short replication) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.setReplication(src, replication);
	  }
	  
	  /**
	   * Renames Path src to Path dst.  Can take place on local fs
	   * or remote DFS.
	   */
	  public boolean rename(Path src, Path dst) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.rename(src, dst);
	  }
	  
	  /** Delete a file */
	  public boolean delete(Path f, boolean recursive) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.delete(f, recursive);
	  }
	  
	  /**
	   * Mark a path to be deleted when FileSystem is closed.
	   * When the JVM shuts down,
	   * all FileSystem objects will be closed automatically.
	   * Then,
	   * the marked path will be deleted as a result of closing the FileSystem.
	   *
	   * The path has to exist in the file system.
	   * 
	   * @param f the path to delete.
	   * @return  true if deleteOnExit is successful, otherwise false.
	   * @throws IOException
	   */
	  public boolean deleteOnExit(Path f) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.deleteOnExit(f);
	  }    

	  /** List files in a directory. */
	  public FileStatus[] listStatus(Path f) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.listStatus(f);
	  }

	  /**
	   * {@inheritDoc}
	   */
	  @Override
	  public RemoteIterator<Path> listCorruptFileBlocks(Path path)
	    throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.listCorruptFileBlocks(path);
	  }

	  /** List files and its block locations in a directory. */
	  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
	  throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.listLocatedStatus(f);
	  }
	  
	  public Path getHomeDirectory() {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.getHomeDirectory();
	  }


	  /**
	   * Set the current working directory for the given file system. All relative
	   * paths will be resolved relative to it.
	   * 
	   * @param newDir
	   */
	  public void setWorkingDirectory(Path newDir) {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

		  super.setWorkingDirectory(newDir);
	  }
	  
	  /**
	   * Get the current working directory for the given file system
	   * 
	   * @return the directory pathname
	   */
	  public Path getWorkingDirectory() {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.getWorkingDirectory();
	  }
	  
	  protected Path getInitialWorkingDirectory() {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.getInitialWorkingDirectory();
	  }
	  
	  /** {@inheritDoc} */
	  @Override
	  public FsStatus getStatus(Path p) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.getStatus(p);
	  }
	  
	  /** {@inheritDoc} */
	  @Override
	  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.mkdirs(f, permission);
	  }


	  /**
	   * The src file is on the local disk.  Add it to FS at
	   * the given dst name.
	   * delSrc indicates if the source should be removed
	   */
	  public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
	    throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

		  super.copyFromLocalFile(delSrc, src, dst);
	  }
	  
	  /**
	   * The src files are on the local disk.  Add it to FS at
	   * the given dst name.
	   * delSrc indicates if the source should be removed
	   */
	  public void copyFromLocalFile(boolean delSrc, boolean overwrite, 
	                                Path[] srcs, Path dst)
	    throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

		  super.copyFromLocalFile(delSrc, overwrite, srcs, dst);
	  }
	  
	  /**
	   * The src file is on the local disk.  Add it to FS at
	   * the given dst name.
	   * delSrc indicates if the source should be removed
	   */
	  public void copyFromLocalFile(boolean delSrc, boolean overwrite, 
	                                Path src, Path dst)
	    throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

		  super.copyFromLocalFile(delSrc, overwrite, src, dst);
	  }

	  /**
	   * The src file is under FS, and the dst is on the local disk.
	   * Copy it from FS control to the local dst name.
	   * delSrc indicates if the src will be removed or not.
	   */   
	  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
	    throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

		  super.copyToLocalFile(delSrc, src, dst);
	  }
	  
	  /**
	   * Returns a local File that the user can write output to.  The caller
	   * provides both the eventual FS target name and the local working
	   * file.  If the FS is local, we write directly into the target.  If
	   * the FS is remote, we write into the tmp local area.
	   */
	  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
	    throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.startLocalOutput(fsOutputFile, tmpLocalFile);
	  }

	  /**
	   * Called when we're all done writing to the target.  A local FS will
	   * do nothing, because we've written to exactly the right place.  A remote
	   * FS will copy the contents of tmpLocalFile to the correct target at
	   * fsOutputFile.
	   */
	  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
	    throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

		  super.completeLocalOutput(fsOutputFile, tmpLocalFile);
	  }

	  /** Return the total size of all files in the filesystem.*/
	  public long getUsed() throws IOException{
	    return super.getUsed();
	  }
	  
	  @Override
	  public long getDefaultBlockSize() {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.getDefaultBlockSize();
	  }
	  
	  @Override
	  public short getDefaultReplication() {
	    return super.getDefaultReplication();
	  }

	  @Override
	  public FsServerDefaults getServerDefaults() throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.getServerDefaults();
	  }

	  // path variants delegate to underlying filesystem 
	  @Override
	  public ContentSummary getContentSummary(Path f) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.getContentSummary(f);
	  }

	  @Override
	  public long getDefaultBlockSize(Path f) {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.getDefaultBlockSize(f);
	  }

	  @Override
	  public short getDefaultReplication(Path f) {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.getDefaultReplication(f);
	  }

	  @Override
	  public FsServerDefaults getServerDefaults(Path f) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.getServerDefaults(f);
	  }

	  /**
	   * Get file status.
	   */
	  public FileStatus getFileStatus(Path f) throws IOException {
		  LOG.info(String.format("getFileStatus(%s)", f.toString()));
	    return super.getFileStatus(f);
	  }

	  /** {@inheritDoc} */
	  public FileChecksum getFileChecksum(Path f) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.getFileChecksum(f);
	  }
	  
	  /** {@inheritDoc} */
	  public void setVerifyChecksum(boolean verifyChecksum) {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    super.setVerifyChecksum(verifyChecksum);
	  }
	  
	  @Override
	  public void setWriteChecksum(boolean writeChecksum) {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    super.setVerifyChecksum(writeChecksum);
	  }

	  @Override
	  public Configuration getConf() {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.getConf();
	  }
	  
	  @Override
	  public void close() throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    super.close();
	  }

	  /** {@inheritDoc} */
	  @Override
	  public void setOwner(Path p, String username, String groupname
	      ) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    super.setOwner(p, username, groupname);
	  }

	  /** {@inheritDoc} */
	  @Override
	  public void setTimes(Path p, long mtime, long atime
	      ) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    super.setTimes(p, mtime, atime);
	  }

	  /** {@inheritDoc} */
	  @Override
	  public void setPermission(Path p, FsPermission permission
	      ) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    super.setPermission(p, permission);
	  }

	  @Override
	  protected FSDataOutputStream primitiveCreate(Path f,
	      FsPermission absolutePermission, EnumSet<CreateFlag> flag,
	      int bufferSize, short replication, long blockSize, Progressable progress, int bytesPerChecksum)
	      throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.primitiveCreate(f, absolutePermission, flag,
	        bufferSize, replication, blockSize, progress, bytesPerChecksum);
	  }

	  @Override
	  @SuppressWarnings("deprecation")
	  protected boolean primitiveMkdir(Path f, FsPermission abdolutePermission)
	      throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.primitiveMkdir(f, abdolutePermission);
	  }
	  
	  @Override // FileSystem
	  public String getCanonicalServiceName() {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.getCanonicalServiceName();
	  }
	  
	  @Override // FileSystem
	  @SuppressWarnings("deprecation")
	  public Token<?> getDelegationToken(String renewer) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.getDelegationToken(renewer);
	  }
	  
	  @Override // FileSystem
	  public List<Token<?>> getDelegationTokens(String renewer) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.getDelegationTokens(renewer);
	  }
	  
	  @Override
	  // FileSystem
	  public List<Token<?>> getDelegationTokens(String renewer,
	      Credentials credentials) throws IOException {
		  LOG.info(Thread.currentThread().getStackTrace()[1].toString());

	    return super.getDelegationTokens(renewer, credentials);
	  }
	
	
}
