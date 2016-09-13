package io.github.dlmarion.clowncar.jnr;

import io.github.dlmarion.clowncar.TestBase;

import java.nio.Buffer;

public class BloscLibraryTest extends TestBase {

	public BloscLibraryTest(String compressor, int level, int shuffle, int threads) {
		super(compressor, level, shuffle, threads);
	}

	@Override
	public int compress(int compressionLevel, int shuffleType, int typeSize,
			Buffer src, long srcLength, Buffer dest, long destLength,
			String compressorName, int blockSize, int numThreads) {
		return BloscLibrary.compress(compressionLevel, shuffleType, typeSize, src, 
				srcLength, dest, destLength, compressorName, blockSize, numThreads);
	}

	@Override
	public int decompress(Buffer src, Buffer dest, long destSize, int numThreads) {
		return BloscLibrary.decompress(src, dest, destSize, numThreads);
	}

}
