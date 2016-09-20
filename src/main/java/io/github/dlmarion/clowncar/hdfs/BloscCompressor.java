package io.github.dlmarion.clowncar.hdfs;

import io.github.dlmarion.clowncar.Blosc;
import io.github.dlmarion.clowncar.BloscCompressorType;
import io.github.dlmarion.clowncar.BloscShuffleType;
import io.github.dlmarion.clowncar.jnr.BloscLibrary;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.DoNotPool;

@DoNotPool
public class BloscCompressor implements Compressor {
	
	public static final String BUFFER_SIZE_KEY = "blosc.compressor.buffer.size";
	public static final String COMPRESSOR_NAME_KEY = "blosc.compressor.name";
	public static final String SHUFFLE_TYPE_KEY = "blosc.compressor.shuffle.type";
	public static final String COMPRESSION_LEVEL_KEY = "blosc.compressor.compression.level";
	public static final String BYTES_FOR_TYPE_KEY = "blosc.compressor.bytes.for.type";
	public static final String COMPRESSED_BLOCK_SIZE_KEY = "blosc.compressor.compressed.block.size";
	public static final String NUM_THREADS_KEY = "blosc.compressor.threads";

	private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
	private static final BloscCompressorType DEFAULT_COMPRESSOR_NAME = BloscCompressorType.ZLIB;
	private static final BloscShuffleType DEFAULT_SHUFFLE_TYPE = BloscShuffleType.NO_SHUFFLE;
	private static final int DEFAULT_COMPRESSION_LEVEL = 6;
	private static final int DEFAULT_BYTES_FOR_TYPE = Long.BYTES;
	private static final int DEFAULT_COMPRESSED_BLOCK_SIZE = 1024;
	private static final int DEFAULT_NUM_THREADS = 2;
	
	private ByteBuffer buffer;
	private int bufferSize = DEFAULT_BUFFER_SIZE;
	private BloscCompressorType compressionType;
	private BloscShuffleType shuffleType;
	private int bytesForType;
	private int compressionLevel;
	private int blockSize;
	private int numThreads;

	private boolean finish = false;
	private long read = 0L;
	private long written = 0L;
	
	public BloscCompressor() {
		this(DEFAULT_BUFFER_SIZE, DEFAULT_COMPRESSOR_NAME, DEFAULT_SHUFFLE_TYPE, DEFAULT_COMPRESSION_LEVEL,
				DEFAULT_BYTES_FOR_TYPE, DEFAULT_COMPRESSED_BLOCK_SIZE, DEFAULT_NUM_THREADS);
	}
	
	public BloscCompressor(Configuration conf) {
		this(conf.getInt(BUFFER_SIZE_KEY, DEFAULT_BUFFER_SIZE), conf);
	}
	
	public BloscCompressor(int bufferSize, Configuration conf) {
		this(bufferSize,
				BloscCompressorType.getCompressorType(conf.get(COMPRESSOR_NAME_KEY, DEFAULT_COMPRESSOR_NAME.getCompressorName())),
				BloscShuffleType.getShuffleType(conf.getInt(SHUFFLE_TYPE_KEY,DEFAULT_SHUFFLE_TYPE.getShuffleType())), 
				conf.getInt(COMPRESSION_LEVEL_KEY, DEFAULT_COMPRESSION_LEVEL),
				conf.getInt(BYTES_FOR_TYPE_KEY, DEFAULT_BYTES_FOR_TYPE),
				conf.getInt(COMPRESSED_BLOCK_SIZE_KEY, DEFAULT_COMPRESSED_BLOCK_SIZE),
				conf.getInt(NUM_THREADS_KEY, DEFAULT_NUM_THREADS));
	}
	
	public BloscCompressor(int bufferSize, BloscCompressorType compressionType, 
			BloscShuffleType shuffleType, int compressionLevel, int bytesForType,
			int blocksize, int numThreads) {
		this.bufferSize = bufferSize;
		this.buffer = ByteBuffer.allocateDirect(bufferSize * 2);
		this.compressionType = compressionType;
		this.shuffleType = shuffleType;
		this.compressionLevel = compressionLevel;
		this.bytesForType = bytesForType;
		this.blockSize = blocksize;
		this.numThreads = numThreads;
	}
	
	@Override
	public void setInput(byte[] b, int off, int len) {
        if (b== null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }
        if (len > buffer.remaining()) {
        	throw new BufferOverflowException();
        }
        buffer.put(b, off, len);
        read += len;
	}

	@Override
	public boolean needsInput() {
		return (buffer.remaining() != 0);
	}

	@Override
	public int compress(byte[] b, int off, int len) throws IOException {
	    if (b == null) {
	        throw new NullPointerException();
	    }
	    if (off < 0 || len < 0 || off > b.length - len) {
	        throw new ArrayIndexOutOfBoundsException();
	    }
	    if (this.buffer.position() == 0) {
	    	return 0;
	    }
	    int bytesToRead = len - off;
		if (bytesToRead <= 0) {
			return 0;
		}
		if (bytesToRead > buffer.position()) {
			bytesToRead = buffer.position();
		}
		int bytesToWrite = bytesToRead + Blosc.OVERHEAD;
		buffer.flip();
		ByteBuffer src = buffer.slice();
		ByteBuffer dst = ByteBuffer.allocate(bytesToWrite);
		int w = BloscLibrary.compress(this.compressionLevel, this.shuffleType.getShuffleType(), this.bytesForType, src, bytesToRead, 
				dst, dst.capacity(), this.compressionType.getCompressorName(), this.blockSize, this.numThreads);
		written += w;
		if (w > b.length) {
			throw new RuntimeException("destination array is not large enough. Currently: " + b.length + ", needs to be: " + w);
		}
		dst.get(b, off, w);
		//We have read src.position() bytes. We need to preserve the bytes left over from the position to the limit.
		buffer.position(bytesToRead);
		buffer.compact();
		return w;
	}

	@Override
	public void setDictionary(byte[] b, int off, int len) {
	}

	@Override
	public long getBytesRead() {
		return read;
	}

	@Override
	public long getBytesWritten() {
		return written;
	}

	@Override
	public void finish() {
		this.finish = true;
	}

	@Override
	public boolean finished() {
		return this.finish && buffer.position() == 0;
	}

	@Override
	public void reset() {
		this.buffer.clear();
		this.finish = false;
		this.read = 0L;
		this.written = 0L;
	}

	@Override
	public void end() {
	}

	@Override
	public void reinit(Configuration conf) {
		this.bufferSize = conf.getInt(BUFFER_SIZE_KEY, DEFAULT_BUFFER_SIZE);
		this.compressionType = BloscCompressorType.getCompressorType(conf.get(COMPRESSOR_NAME_KEY, DEFAULT_COMPRESSOR_NAME.getCompressorName()));
		this.shuffleType = BloscShuffleType.getShuffleType(conf.getInt(SHUFFLE_TYPE_KEY,DEFAULT_SHUFFLE_TYPE.getShuffleType())); 
		this.compressionLevel = conf.getInt(COMPRESSION_LEVEL_KEY, DEFAULT_COMPRESSION_LEVEL);
		this.bytesForType = conf.getInt(BYTES_FOR_TYPE_KEY, DEFAULT_BYTES_FOR_TYPE);
		this.blockSize = conf.getInt(COMPRESSED_BLOCK_SIZE_KEY, DEFAULT_COMPRESSED_BLOCK_SIZE);
		this.numThreads = conf.getInt(NUM_THREADS_KEY, DEFAULT_NUM_THREADS);
		reset();
		if (this.bufferSize != this.buffer.capacity()) {
			buffer = ByteBuffer.allocateDirect(bufferSize);
		}
	}

}
