package io.github.dlmarion.clowncar.io;

import static java.nio.charset.StandardCharsets.UTF_8;
import io.github.dlmarion.clowncar.Blosc;
import io.github.dlmarion.clowncar.BloscCompressorType;
import io.github.dlmarion.clowncar.BloscShuffleType;
import io.github.dlmarion.clowncar.jnr.BloscLibrary;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BloscOutputStream<T> extends OutputStream implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(BloscOutputStream.class);
	
	private final ThreadLocal<ByteBuffer> SIZE = new ThreadLocal<ByteBuffer>() {
		@Override
		protected ByteBuffer initialValue() {
	        return ByteBuffer.allocate(Integer.BYTES);
	    }
	};
	
	private final OutputStream out;
	private final ByteBuffer buf;
	private final ByteBuffer dst;
	private final Class<T> type;
	private final BloscCompressorType compressor;
	private final int compressionLevel;
	private final BloscShuffleType shuffleType;
	private final int numThreads;
	private final int typeSize;
	
	public BloscOutputStream(OutputStream out, int blocksize, Class<T> type, BloscCompressorType compressor, int compressionLevel, BloscShuffleType shuffleType, int numThreads) {
		this.out = out;
		this.buf = ByteBuffer.allocateDirect(blocksize);
		this.dst = ByteBuffer.allocateDirect(blocksize + Blosc.OVERHEAD);
		this.type = type;
		this.compressor = compressor;
		this.compressionLevel = compressionLevel;
		this.shuffleType = shuffleType;
		this.numThreads = numThreads;
		if (type.equals(Long.class)) {
			this.typeSize = Long.BYTES;
		} else if (type.equals(Integer.class)) {
			this.typeSize = Integer.BYTES;
		} else if (type.equals(Double.class)) {
			this.typeSize = Double.BYTES;
		} else if (type.equals(Float.class)) {
			this.typeSize = Float.BYTES;
		} else {
			this.typeSize = 256; //disables shuffle
		}
	}
	
	private void writeBuffer() throws IOException {
		if (this.buf.position() == 0) {
			return;
		}
		int srcLength = this.buf.position();
		this.buf.position(0);
		int written = BloscLibrary.compress(this.compressionLevel, this.shuffleType.getShuffleType(), this.typeSize,
				this.buf, srcLength, this.dst, this.dst.capacity(), this.compressor.getCompressorName(), this.buf.capacity(), this.numThreads);
		LOG.trace("buf size: {}, wrote: {}, level: {}, compression: {}", srcLength, written, this.compressionLevel, (written*1.0D/srcLength));
		//write compressed size
		SIZE.get().putInt(written);
		out.write(SIZE.get().array());
		SIZE.get().clear();
		//write uncompressed size
		SIZE.get().putInt(srcLength);
		out.write(SIZE.get().array());
		SIZE.get().clear();
		//write compressed data
		byte[] copy = new byte[written];
		this.dst.get(copy);
		out.write(copy, 0, written);
		this.buf.clear();
		this.dst.clear();
	}
	
	public void write(T in) throws IOException {
		int toWrite = 0;
		if (this.typeSize != 0) {
			toWrite = this.typeSize;
		}
		
		if (this.buf.remaining() < toWrite) {
			writeBuffer();
		}
		
		if (type.equals(Long.class)) {
			buf.putLong((Long)in);
		} else if (type.equals(Integer.class)) {
			buf.putInt((Integer)in);
		} else if (type.equals(Double.class)) {
			buf.putDouble((Double)in);
		} else if (type.equals(Float.class)) {
			buf.putFloat((Float)in);
		} else if (type.equals(String.class)) {
			byte[] b  = ((String) in).getBytes(UTF_8);
			if (this.buf.remaining() < b.length) {
				writeBuffer();
			}
			buf.putInt(b.length);
			buf.put(b);
		}
	}
	
	@Override
	public void write(int b) throws IOException {
		throw new IOException("Unsupported operation.");
	}

	@Override
	public void flush() throws IOException {
		writeBuffer();
		super.flush();
	}

	@Override
	public void close() throws IOException {
		flush();
		this.buf.clear();
		this.dst.clear();
		super.close();
		out.close();
	}

}
