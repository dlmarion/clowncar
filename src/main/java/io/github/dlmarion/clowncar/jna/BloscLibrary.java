package io.github.dlmarion.clowncar.jna;

import java.nio.Buffer;

import com.sun.jna.Native;

public class BloscLibrary {
	
	static {
		Native.register("blosc");
	}

	public static int compress(int compressionLevel, int shuffleType,
			int typeSize, Buffer src, long srcLength, Buffer dest, 
			long destLength, String compressorName, int blockSize, int numThreads) {
		if (typeSize > 255) {
			throw new IllegalArgumentException("Type size must be below 256");
		}
		if (srcLength > (Integer.MAX_VALUE - 16)) {
			throw new IllegalArgumentException("Source array is too large");
		}
		if (destLength < (srcLength + 16)) {
			throw new IllegalArgumentException(
					"Dest array is not large enough.");
		}
		return blosc_compress_ctx(compressionLevel, shuffleType, new SizeT(typeSize), new SizeT(srcLength), 
				src, dest, new SizeT(destLength), compressorName, new SizeT(blockSize), numThreads);
	}
	
	public static int decompress(Buffer src, Buffer dest, long destSize, int numThreads) {
		return blosc_decompress_ctx(src, dest, new SizeT(destSize), numThreads);
	}


	static native int blosc_compress_ctx(int compressionLevel,
			               int shuffleType,
			               SizeT typesize,
			               SizeT nbytes,
			               Buffer src,
			               Buffer dest,
			               SizeT destsize,
			               String compressorName,
			               SizeT blockSize,
			               int numThreads);

	public static native int blosc_decompress_ctx(Buffer src, Buffer dest, SizeT destsize, int numinternalthreads);

}
