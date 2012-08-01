package cn.ict.magicube.math;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ServiceLoader;

import org.apache.hadoop.util.Options;

public class RaidNK extends RaidAlgorithm {
	private static int vectorMul(GaloisField GF, int[] v1, int[] v2) {
		assert v1.length == v2.length;
		int v = 0;
		for (int i = 0; i < v1.length; i++) {
			v = GF.add(v,
					GF.multiply((int)(v1[i] & 0xFF),
							    (int)(v2[i] & 0xFF))
				);
		}
		return v;
	}
	
	private int K;
	final GaloisField GF;
	RaidNK(RaidAlgorithm.Option ... options) {
		super();
		try {
			K = Options.getOption(RaidAlgorithm.KOption.class, options).getValue();
		} catch (IOException e) {
			throw new IllegalArgumentException("K is not set");
		}
		GF = GaloisField.getInstance();
	}
	
	@Override
	public void encode(InputStream origin, OutputStream[] parities,
			int[] parityNums, long originLength, OutputStream copyStream) throws IOException {
		
		int[][] pivots = new int[parityNums.length][];
		for (int i = 0; i < parityNums.length; i++) {
			pivots[i] = GF.makePivot(parityNums[i], K);
		}
		
		int bytesProcessed = 0;
		int[] buf = new int[K];
		while (bytesProcessed < originLength) {
			// read K bytes
			long leftBytes = originLength - bytesProcessed;
			int i;
			for (i = 0; i < Math.min(K, leftBytes); i++) {
				try {
					buf[i] = origin.read() & 0xFF;
					if (copyStream != null)
						copyStream.write(buf[i]);
				} catch (IOException e) {
				}
			}
			bytesProcessed += i;
			
			for (i = 0; i < parityNums.length; i++) {
				int v = vectorMul(GF, buf, pivots[i]);
				parities[i].write(v);
			}
		}
	}

	@Override
	public void decode(OutputStream origin, InputStream[] parities,
			int[] parityNums, long originLength) throws IOException {
		if (parityNums.length != parities.length)
			throw new IllegalArgumentException(
					String.format("parityNums.length(%d) and parities.length(%d) not match",
							parityNums.length, parities.length)
					);
		
		int[][] reversedVandermonde = GF.reverseVandermonde(parityNums);
		
		for (int i = 0; i < reversedVandermonde.length - 1; i++) {
			for (int j = i + 1; j < reversedVandermonde.length; j++) {
				int t = reversedVandermonde[i][j];
				reversedVandermonde[i][j] = reversedVandermonde[j][i];
				reversedVandermonde[j][i] = t;
			}
		}
		
		
		int[] v = new int[parityNums.length];
		int[] buf = new int[reversedVandermonde.length];
		long recoveredBytes = 0;
		while (recoveredBytes < originLength) {
			for (int i = 0; i < parities.length; i++) {
				v[i] = parities[i].read();
			}
			for (int i = 0; i < reversedVandermonde.length; i++) {
				buf[i] = vectorMul(GF, v, reversedVandermonde[i]);
			}
			
			long n = Math.min(originLength - recoveredBytes,
					buf.length);
			for (int i = 0; i < n; i++)
				origin.write(buf[i]);
			recoveredBytes += n;
		}
	}
	
	public static class NoSuchRaidAlgorithmException extends IllegalArgumentException {
		private static final long serialVersionUID = -3638808367358470957L;		
	}

	
	public static void main(String[] args) throws NoSuchRaidAlgorithmException, IOException {
		RaidAlgorithm nk = RaidAlgorithm.load(
				RaidAlgorithm.name("nk"),
				RaidAlgorithm.n(5),
				RaidAlgorithm.k(3)
				);
		if (nk == null)
			throw new NoSuchRaidAlgorithmException();
		
		byte[] buf = new byte[25];

		for (int i = 0; i < buf.length; i++) {
			buf[i] = (byte)((i + 1) & 0xff);
		}
		
		ByteArrayInputStream srcis = new ByteArrayInputStream(buf);
		ByteArrayOutputStream[] paritiesOS = new ByteArrayOutputStream[5];
		paritiesOS[0] = new ByteArrayOutputStream();
		paritiesOS[1] = new ByteArrayOutputStream();
		paritiesOS[2] = new ByteArrayOutputStream();
		paritiesOS[3] = new ByteArrayOutputStream();
		paritiesOS[4] = new ByteArrayOutputStream();
		
		int[] parityNums = new int[5];
		for (int i = 0; i < parityNums.length; i++)
			parityNums[i] = i + 2;
		
		nk.encode(srcis, paritiesOS, parityNums, 25, null);
		srcis.close();
		for (int i = 0; i < paritiesOS.length; i++)
			paritiesOS[i].close();
		
		for (int i = 0; i < paritiesOS.length; i++) {
			byte[] array = paritiesOS[i].toByteArray();
			for (byte b : array) {
				System.out.format("%02x ", b);
			}
			System.out.format("\n");
		}
		
		ByteArrayInputStream[] paritiesIS = new ByteArrayInputStream[3];
		paritiesIS[0] = new ByteArrayInputStream(paritiesOS[0].toByteArray());
		paritiesIS[1] = new ByteArrayInputStream(paritiesOS[1].toByteArray());
		paritiesIS[2] = new ByteArrayInputStream(paritiesOS[2].toByteArray());
		
		int[] parityNums2 = new int[3];
		parityNums2[0] = 2;
		parityNums2[1] = 3;
		parityNums2[2] = 4;
		ByteArrayOutputStream originOS = new ByteArrayOutputStream();
		nk.decode(originOS, paritiesIS, parityNums2, 25);
		
		byte[] buf2 = originOS.toByteArray();
		for (int i = 0; i < buf2.length; i++) {
			System.out.format("%02x ", buf2[i]);
		}
		System.out.println();
	}
	
}
