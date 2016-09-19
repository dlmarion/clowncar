package io.github.dlmarion.clowncar.hdfs;

import io.github.dlmarion.clowncar.jnr.BloscLibrary;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.apache.hadoop.io.compress.DoNotPool;

@DoNotPool
public class BloscDecompressor implements Decompressor, DirectDecompressor {

	public static final String BUFFER_SIZE_KEY = "blosc.decompressor.buffer.size";
	public static final String NUM_THREADS_KEY = "blosc.decompressor.threads";
	private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
	private static final int DEFAULT_NUM_THREADS = 2;

	private final ByteBuffer buffer;
	private final int numThreads;

	public BloscDecompressor() {
		this(64*1024);
	}

	public BloscDecompressor(int bufferSize) {
		buffer = ByteBuffer.allocateDirect(bufferSize);
		this.numThreads = DEFAULT_NUM_THREADS;
	}
	
	public BloscDecompressor(Configuration conf) {
		buffer = ByteBuffer.allocateDirect(conf.getInt(BUFFER_SIZE_KEY, DEFAULT_BUFFER_SIZE));
		this.numThreads = conf.getInt(NUM_THREADS_KEY, DEFAULT_NUM_THREADS);

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
	}

	@Override
	public boolean needsInput() {
		return (buffer.remaining() != 0);
	}

	@Override
	public void setDictionary(byte[] b, int off, int len) {
	}

	@Override
	public boolean needsDictionary() {
		return false;
	}

	@Override
	public boolean finished() {
		return buffer.position() == 0;
	}

	@Override
	public int decompress(byte[] b, int off, int len) throws IOException {
	    if (b == null) {
	        throw new NullPointerException();
	    }
	    if (off < 0 || len < 0 || off > b.length - len) {
	        throw new ArrayIndexOutOfBoundsException();
	    }
	    if (this.buffer.position() == 0) {
	    	return 0;
	    }
	    //Read b.length-16 bytes from buffered input
	    int bytesToRead = len - off;
		if (bytesToRead <= 0) {
			return 0;
		}
		if (bytesToRead > buffer.position()) {
			bytesToRead = buffer.position();
		}
		buffer.flip();
		ByteBuffer src = buffer.slice();
		ByteBuffer dst = ByteBuffer.allocate(len);
		int r = BloscLibrary.decompress(src, dst, dst.capacity(), this.numThreads);
		if (r == -1) {
			throw new RuntimeException("Error decompressing data: src: " + src + ", dst: " + dst + ", toRead: " + bytesToRead);
		}
		dst.get(b, off, r);
		//We have read src.position() bytes. We need to preserve the bytes left over from the position to the limit.
		buffer.position(bytesToRead);
		buffer.compact();
		return r;
	}

	@Override
	public int getRemaining() {
		return buffer.position();
	}

	@Override
	public void reset() {
		buffer.clear();
	}

	@Override
	public void end() {
	}

	@Override
	public void decompress(ByteBuffer src, ByteBuffer dst) throws IOException {
		BloscLibrary.decompress(src, dst, dst.remaining(), this.numThreads);
	}

}
