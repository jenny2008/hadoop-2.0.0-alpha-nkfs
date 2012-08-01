package cn.ict.magicube.math;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ServiceLoader;

import org.apache.hadoop.util.Options;

public abstract class RaidAlgorithm {
	public static interface Option {}
	static class NameOption extends Options.StringOption
		implements Option {
		NameOption(String value) {
			super(value);
		}
	}
	static class NOption extends Options.IntegerOption
		implements Option {
		NOption(int value) {
			super(value);
		}
	}
	static class KOption extends Options.IntegerOption
		implements Option {
		KOption(int value) {
			super(value);
		}
	}
	
	public static Option name(String value) {
		return new NameOption(value);
	}
	public static Option n(int value) {
		return new NOption(value);
	}
	public static Option k(int value) {
		return new KOption(value);
	}

	
	protected RaidAlgorithm() {}
	
	public abstract void encode(InputStream origin, OutputStream[] parities,
			int[] parityNums, long originLength, OutputStream copyStream) throws IOException;
	public abstract void decode(OutputStream origin, InputStream[] parities,
			int[] parityNums, long originLength) throws IOException;
	
	public static class NoSuchRaidAlgorithmException extends IllegalArgumentException {
		private static final long serialVersionUID = -3638808367358470957L;		
	}

	public static RaidAlgorithm load(RaidAlgorithm.Option ... options) {
		RaidAlgorithm algo = null;
		ServiceLoader<RaidAlgorithmProvider> loader = ServiceLoader.load(RaidAlgorithmProvider.class);
		for (RaidAlgorithmProvider p : loader) {
			algo = p.create(options);
			if (algo != null)
				break;
		}
		
		if (algo == null)
			throw new NoSuchRaidAlgorithmException();
		return algo;
	}
}
