package io.github.dlmarion.clowncar.jnr;

import java.nio.Buffer;

import jnr.ffi.LibraryLoader;
import jnr.ffi.types.size_t;

public class BloscLibrary {
		
	private static final libBlosc INSTANCE = LibraryLoader.create(libBlosc.class).load("blosc");

	public interface libBlosc {
		
		public int blosc_compress_ctx(int compressionLevel,
				int shuffleType,
				@size_t long typesize,
				@size_t long nbytes,
				Buffer src,
				Buffer dest,
				@size_t long destsize,
				String compressorName,
				@size_t long blockSize,
				int numThreads);
		
		public int blosc_decompress_ctx(Buffer src,
				Buffer dest,
				@size_t long destsize,
				int numinternalthreads);
	}

	public static int compress(int compressionLevel, int shuffleType,
			int typeSize, Buffer src, long srcLength, Buffer dest, 
			long destLength, String compressorName, int blockSize, int numThreads) {
		if (srcLength > (Integer.MAX_VALUE - 16)) {
			throw new IllegalArgumentException("Source array is too large");
		}
		if (destLength < (srcLength + 16)) {
			throw new IllegalArgumentException(
					"Dest array is not large enough.");
		}
		return INSTANCE.blosc_compress_ctx(compressionLevel, shuffleType, typeSize, srcLength, 
				src, dest, destLength, compressorName, blockSize, numThreads);
	}
	
	public static int decompress(Buffer src, Buffer dest, long destSize, int numThreads) {
		return INSTANCE.blosc_decompress_ctx(src, dest, destSize, numThreads);
	}

}
