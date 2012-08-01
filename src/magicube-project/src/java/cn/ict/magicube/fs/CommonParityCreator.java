package cn.ict.magicube.fs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CommonParityCreator extends ParityCreator {
	static final Log LOG = LogFactory.getLog(CommonParityCreator.class);

	private List<OutputStream> _streams;
	private OutputStream _originStream;
	public CommonParityCreator(FileSystem baseFS, Path partDir) {
		super(baseFS, partDir);
		reset();
		LOG.info("fall back to CommonParityCreator");
	}
	
	@Override
	public void reset() {
		_streams = new LinkedList<OutputStream>();
		_originStream = null;
	}

	@Override
	public OutputStream[] getOutputStreams() throws IOException {
		OutputStream[] streams_a = new OutputStream[_streams.size()];
		_streams.toArray(streams_a);
		return streams_a;
	}

	@Override
	public void addOutputPath(Path filePath, boolean isOrigin)
			throws IOException {
		OutputStream os = _baseFS.create(filePath);
		if (isOrigin) {
			if (_originStream != null)
				_originStream.close();
			_originStream = os;
			return;
		} else {
			_streams.add(os);
		}
	}

	@Override
	public OutputStream getOriginOutputStream() throws IOException {
		return _originStream;
	}
}
