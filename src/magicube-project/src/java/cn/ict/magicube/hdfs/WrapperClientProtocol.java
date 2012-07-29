package cn.ict.magicube.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.UpgradeAction;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;

import cn.ict.magicube.spikes.fs.NKFileSystem;

public class WrapperClientProtocol implements ClientProtocol {
	static final Log LOG = LogFactory.getLog(WrapperClientProtocol.class);

	private ClientProtocol wrapped;
	List<DatanodeInfo> excludeNodes_predefined;
	
	public WrapperClientProtocol(ClientProtocol wrapped) {
		this.wrapped = wrapped;
		excludeNodes_predefined = new LinkedList<DatanodeInfo>();
	}
	
	public void addExcludeNodes(List<DatanodeInfo> ex) {
		for (DatanodeInfo newdi : ex) {
			addExcludeNode(newdi);
		}
	}
	
	public void addExcludeNode(DatanodeInfo newdi) {
		boolean equal = false;
		for (DatanodeInfo olddi : excludeNodes_predefined) {
			if (newdi.compareTo(olddi) == 0) {
				equal = true;
				break;
			}
		}
		if (!equal)
			excludeNodes_predefined.add(newdi);
	}
	
	public void resetExcludeNodes() {
		excludeNodes_predefined = new LinkedList<DatanodeInfo>();
	}

	@Override
	public LocatedBlocks getBlockLocations(String src, long offset, long length)
			throws AccessControlException, FileNotFoundException,
			UnresolvedLinkException, IOException {
		return wrapped.getBlockLocations(src, offset, length);
	}

	@Override
	public FsServerDefaults getServerDefaults() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void create(String src, FsPermission masked, String clientName,
			EnumSetWritable<CreateFlag> flag, boolean createParent,
			short replication, long blockSize) throws AccessControlException,
			AlreadyBeingCreatedException, DSQuotaExceededException,
			FileAlreadyExistsException, FileNotFoundException,
			NSQuotaExceededException, ParentNotDirectoryException,
			SafeModeException, UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public LocatedBlock append(String src, String clientName)
			throws AccessControlException, DSQuotaExceededException,
			FileNotFoundException, SafeModeException, UnresolvedLinkException,
			IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean setReplication(String src, short replication)
			throws AccessControlException, DSQuotaExceededException,
			FileNotFoundException, SafeModeException, UnresolvedLinkException,
			IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setPermission(String src, FsPermission permission)
			throws AccessControlException, FileNotFoundException,
			SafeModeException, UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setOwner(String src, String username, String groupname)
			throws AccessControlException, FileNotFoundException,
			SafeModeException, UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void abandonBlock(ExtendedBlock b, String src, String holder)
			throws AccessControlException, FileNotFoundException,
			UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public LocatedBlock addBlock(String src, String clientName,
			ExtendedBlock previous, DatanodeInfo[] excludeNodes)
			throws AccessControlException, FileNotFoundException,
			NotReplicatedYetException, SafeModeException,
			UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LocatedBlock getAdditionalDatanode(String src, ExtendedBlock blk,
			DatanodeInfo[] existings, DatanodeInfo[] excludes,
			int numAdditionalNodes, String clientName)
			throws AccessControlException, FileNotFoundException,
			SafeModeException, UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean complete(String src, String clientName, ExtendedBlock last)
			throws AccessControlException, FileNotFoundException,
			SafeModeException, UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean rename(String src, String dst)
			throws UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void concat(String trg, String[] srcs) throws IOException,
			UnresolvedLinkException {
		// TODO Auto-generated method stub

	}

	@Override
	public void rename2(String src, String dst, Rename... options)
			throws AccessControlException, DSQuotaExceededException,
			FileAlreadyExistsException, FileNotFoundException,
			NSQuotaExceededException, ParentNotDirectoryException,
			SafeModeException, UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean delete(String src, boolean recursive)
			throws AccessControlException, FileNotFoundException,
			SafeModeException, UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean mkdirs(String src, FsPermission masked, boolean createParent)
			throws AccessControlException, FileAlreadyExistsException,
			FileNotFoundException, NSQuotaExceededException,
			ParentNotDirectoryException, SafeModeException,
			UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public DirectoryListing getListing(String src, byte[] startAfter,
			boolean needLocation) throws AccessControlException,
			FileNotFoundException, UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void renewLease(String clientName) throws AccessControlException,
			IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean recoverLease(String src, String clientName)
			throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long[] getStats() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getPreferredBlockSize(String filename) throws IOException,
			UnresolvedLinkException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean setSafeMode(SafeModeAction action) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void saveNamespace() throws AccessControlException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean restoreFailedStorage(String arg)
			throws AccessControlException, IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void refreshNodes() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void finalizeUpgrade() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void metaSave(String filename) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBalancerBandwidth(long bandwidth) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public HdfsFileStatus getFileInfo(String src)
			throws AccessControlException, FileNotFoundException,
			UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HdfsFileStatus getFileLinkInfo(String src)
			throws AccessControlException, UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ContentSummary getContentSummary(String path)
			throws AccessControlException, FileNotFoundException,
			UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setQuota(String path, long namespaceQuota, long diskspaceQuota)
			throws AccessControlException, FileNotFoundException,
			UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void fsync(String src, String client) throws AccessControlException,
			FileNotFoundException, UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setTimes(String src, long mtime, long atime)
			throws AccessControlException, FileNotFoundException,
			UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void createSymlink(String target, String link, FsPermission dirPerm,
			boolean createParent) throws AccessControlException,
			FileAlreadyExistsException, FileNotFoundException,
			ParentNotDirectoryException, SafeModeException,
			UnresolvedLinkException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public String getLinkTarget(String path) throws AccessControlException,
			FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LocatedBlock updateBlockForPipeline(ExtendedBlock block,
			String clientName) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updatePipeline(String clientName, ExtendedBlock oldBlock,
			ExtendedBlock newBlock, DatanodeID[] newNodes) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
			throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
			throws IOException {
		// TODO Auto-generated method stub

	}

}
