package org.apache.hadoop.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import cn.ict.magicube.fs.ParityCreator;
import cn.ict.magicube.fs.ParityCreatorProvider;
import cn.ict.magicube.fs.ParityCreator.Option;

public class HDFSParityCreatorProvider extends ParityCreatorProvider {
	@Override
	public ParityCreator create(FileSystem baseFS, Path partDirPath,
			Option... options) {
		try {
			if (baseFS == null)
				return null;
			if (partDirPath == null)
				return null;
			if (baseFS instanceof DistributedFileSystem)
				return new HDFSParityCreator((DistributedFileSystem)baseFS, partDirPath);
		} catch (Exception e) {
			e.printStackTrace();	
		}
		return null;
	}
}
