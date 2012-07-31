package cn.ict.magicube.math;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public abstract class RaidAlgorithm {
	
	public abstract static class RaidAlgorithmOption {
	}
	public RaidAlgorithm(RaidAlgorithmOption[] options) {
	}
	
	public abstract void encode(InputStream origin, OutputStream[] parities,
			int[] parityNums, long originLength, OutputStream copyStream) throws IOException;
	public abstract void decode(OutputStream origin, InputStream[] parities,
			int[] parityNums, long originLength) throws IOException;
	
	public static class NoSuchRaidAlgorithmException extends IllegalArgumentException {
		private static final long serialVersionUID = -3638808367358470957L;		
	}
	
	public static RaidAlgorithm getRaidAlgorithm(String algo, RaidAlgorithmOption ... options) throws NoSuchRaidAlgorithmException {
		if (algo == "nk")
			return new RaidNK(options);
		throw new NoSuchRaidAlgorithmException();
	} 
}
