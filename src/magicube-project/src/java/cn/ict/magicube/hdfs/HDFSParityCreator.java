package cn.ict.magicube.hdfs;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.ict.magicube.fs.ParityCreator;

public class HDFSParityCreator extends ParityCreator {

	public HDFSParityCreator(FileSystem baseFS, Path partDir) {
		super(baseFS, partDir);
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
