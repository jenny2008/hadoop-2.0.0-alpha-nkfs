package cn.ict.magicube.fs;

import java.io.IOException;

import java.io.OutputStream;
import java.util.ServiceLoader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Options;

public abstract class ParityCreator {
	protected final FileSystem _baseFS;
	protected final Path _partDirPath;

	public void addOutputPath(String filename, boolean isOrigin) throws IOException {
		Path filePath = new Path(_partDirPath, filename);
		addOutputPath(filePath, isOrigin);
	}
	
	//public abstract OutputStream createStream(Path filePath) throws IOException;
	public abstract void addOutputPath(Path filePath, boolean isOrigin) throws IOException;
	public abstract OutputStream[] getOutputStreams() throws IOException;
	public abstract OutputStream getOriginOutputStream() throws IOException;
	public abstract void reset();
	 
	
	protected ParityCreator(FileSystem baseFS, Path partDirPath) {
		_baseFS = baseFS;
		_partDirPath = partDirPath;
	}

	public static interface Option {}
	static class NameOption extends Options.StringOption
		implements Option {
			NameOption(String name) {
				super(name);
			}
	}

	public static Option name(String name) {
		return new NameOption(name);
	}
	

	
	public static class NoSuchCreatorException extends IllegalArgumentException {
		private static final long serialVersionUID = 1935313055210061417L;
	}
	public static ParityCreator load(FileSystem baseFS,
			Path partDirPath,
			Option ... opts) {
		ParityCreator creator = null;
		ServiceLoader<ParityCreatorProvider> loader = ServiceLoader.load(ParityCreatorProvider.class);
		for (ParityCreatorProvider p : loader) {
			if (p instanceof CommonParityCreatorProvider)
				continue;
			creator = p.create(baseFS, partDirPath, opts);
			if (creator != null)
				break;
		}
		if (creator == null)
			creator = new CommonParityCreatorProvider().create(
					baseFS,
					partDirPath,
					opts);
		if (creator == null)
			throw new NoSuchCreatorException();
		return creator;
	}
}
