package cn.ict.magicube.fs;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CommonParityCreator extends ParityCreator {
	public CommonParityCreator(FileSystem baseFS, Path partDir) {
		super(baseFS, partDir);
	}

	@Override
	public OutputStream createStream(Path filePath) throws IOException {
		OutputStream os = _baseFS.create(filePath);
		_baseFS.setReplication(filePath, (short)1);
		return os;
	}

	@Override
	public void reset() {
	}

}
