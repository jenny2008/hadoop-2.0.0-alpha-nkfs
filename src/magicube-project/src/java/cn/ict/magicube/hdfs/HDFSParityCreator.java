package cn.ict.magicube.hdfs;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import cn.ict.magicube.fs.ParityCreator;

public class HDFSParityCreator extends ParityCreator {
	private DistributedFileSystem _dfs;
	public HDFSParityCreator(FileSystem baseFS, Path partDir) throws IOException {
		super(baseFS, partDir);
		_dfs = (DistributedFileSystem)baseFS;
		FileStatus[] existFiles = baseFS.listStatus(partDir);
		for (FileStatus stat : existFiles) {
			BlockLocation[] bls = baseFS.getFileBlockLocations(stat, 0, stat.getLen());
			for (BlockLocation bl : bls) {
				
			}
		}
	}

	@Override
	public OutputStream createStream(Path filePath) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub		
	}
}
