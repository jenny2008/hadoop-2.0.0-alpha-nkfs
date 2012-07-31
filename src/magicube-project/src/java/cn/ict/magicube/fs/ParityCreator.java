package cn.ict.magicube.fs;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import cn.ict.magicube.hdfs.HDFSParityCreator;

public abstract class ParityCreator {
	protected final FileSystem _baseFS;
	protected final Path _partDir;

	public OutputStream createStream(String filename) throws IOException {
		Path filePath = new Path(_partDir, filename);
		return createStream(filePath);
	}
	
	public abstract OutputStream createStream(Path filePath) throws IOException;
	public abstract void reset();
	
	public ParityCreator(FileSystem baseFS, Path partDir) {
		_baseFS = baseFS;
		_partDir = partDir;
	}
	
	private static class TrivialParityCreator extends ParityCreator {
		public TrivialParityCreator(FileSystem baseFS, Path partDir) {
			super(baseFS, partDir);
		}

		@Override
		public OutputStream createStream(Path filepath) throws IOException {
			OutputStream os = _baseFS.create(filepath);
			_baseFS.setReplication(filepath, (short)1);
			return os;
		}

		@Override
		public void reset() {
		}	
	}
	
	public static ParityCreator getInstance(FileSystem baseFS, Path partDir) {
		return new TrivialParityCreator(baseFS, partDir);
	}
}
