package io.github.dlmarion.clowncar;

import com.sun.jna.Native;

public class BloscLibrary {
	
	static {
		Native.register("blosc");
	}

	public static final String BLOSCLZ = "blosclz";
	public static final String LZ4 = "lz4";
	public static final String LZ4HC = "lz4hc";
	public static final String SNAPPY = "snappy";
	public static final String ZLIB = "zlib";
	public static final String ZSTD = "zstd";

	public static final int NO_SHUFFLE = 0;
	public static final int BYTE_SHUFFLE = 1;
	public static final int BIT_SHUFFLE = 2;
	
	public static int compress(int compressionLevel,
			                   int shuffleType,
			                   int typeSize,
			                   byte[] src,
			                   byte[] dest,
			                   String compressorName,
			                   int blockSize,
			                   int numThreads) {
		if (typeSize > 255) {
			throw new IllegalArgumentException("Type size must be below 256");
		}
		if (src.length > (Integer.MAX_VALUE - 16)) {
			throw new IllegalArgumentException("Source array is too large");
		}
		if (dest.length < (src.length + 16)) {
			throw new IllegalArgumentException("Dest array is not large enough.");
		}
		return blosc_compress_ctx(compressionLevel, shuffleType,
				new SizeT(typeSize), new SizeT(src.length), src, dest, 
				new SizeT(dest.length), compressorName, new SizeT(blockSize), numThreads);
	}

	static native int blosc_compress_ctx(int compressionLevel,
			               int shuffleType,
			               SizeT typesize,
			               SizeT nbytes,
			               byte[] src,
			               byte[] dest,
			               SizeT destsize,
			               String compressorName,
			               SizeT blockSize,
			               int numThreads);

	public static native int blosc_decompress_ctx(byte[] src,
			                 byte[] dest,
			                 SizeT destsize,
			                 int numinternalthreads);

}
