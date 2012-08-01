package cn.ict.magicube.fs;

import java.io.IOException;

import java.io.OutputStream;
import java.util.ServiceLoader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Options;

public abstract class ParityCreator {
	public static interface Option {}
	static class BaseFSOption extends Options
		implements Option {
		private final FileSystem _baseFS;
		public FileSystem getValue() {
			return _baseFS;
		}
		BaseFSOption(FileSystem baseFS) {
			_baseFS = baseFS;
		}

	}
	static class PartDirOption extends Options.PathOption
		implements Option {
		PartDirOption(Path partDir) {
			super(partDir);
		}
	}
	static class NameOption extends Options.StringOption
		implements Option {
			NameOption(String name) {
				super(name);
			}
	}

	public static Option baseFS(FileSystem baseFS) {
		return new BaseFSOption(baseFS);
	}
	public static Option partDir(Path partDir) {
		return new PartDirOption(partDir);
	}
	
	protected final FileSystem _baseFS;
	protected final Path _partDir;

	public OutputStream createStream(String filename) throws IOException {
		Path filePath = new Path(_partDir, filename);
		return createStream(filePath);
	}
	
	public abstract OutputStream createStream(Path filePath) throws IOException;
	public abstract void reset();
	
	protected ParityCreator(FileSystem baseFS, Path partDir) {
		_baseFS = baseFS;
		_partDir = partDir;
	}
	
	public static class NoSuchCreatorException extends IllegalArgumentException {
		private static final long serialVersionUID = 1935313055210061417L;
	}
	public static ParityCreator load(Option ... opts) {
		ParityCreator creator = null;
		ServiceLoader<ParityCreatorProvider> loader = ServiceLoader.load(ParityCreatorProvider.class);
		for (ParityCreatorProvider p : loader) {
			creator = p.create(opts);
			if (creator != null)
				break;
		}
		if (creator == null)
			throw new NoSuchCreatorException();
		return creator;
	}
}
