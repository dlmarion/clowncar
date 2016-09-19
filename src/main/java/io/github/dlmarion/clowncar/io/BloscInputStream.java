package io.github.dlmarion.clowncar.io;

import static java.nio.charset.StandardCharsets.UTF_8;
import io.github.dlmarion.clowncar.jnr.BloscLibrary;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class BloscInputStream<T> extends InputStream implements AutoCloseable {

	private final InputStream in;
	private final int numThreads;
	private final Class<T> type;
	private ByteBuffer buf;
	
	public BloscInputStream(InputStream in, Class<T> type, int numThreads) {
		this.in = in;
		this.type = type;
		this.numThreads = numThreads;
	}
	
	private void fillBuffer() throws IOException {
		//Read compressed size
		ByteBuffer compressedSize = ByteBuffer.allocate(4);
		in.read(compressedSize.array());
		//Read uncompressed size
		ByteBuffer unCompressedSize = ByteBuffer.allocate(4);
		in.read(unCompressedSize.array());
		//Read compressed data
		ByteBuffer src = ByteBuffer.allocate(compressedSize.getInt());
		in.read(src.array());
		this.buf = ByteBuffer.allocate(unCompressedSize.getInt());
		@SuppressWarnings("unused") //may do something with this later
		int read = BloscLibrary.decompress(src, buf, buf.capacity(), this.numThreads); 
	}
	
	@SuppressWarnings("unchecked")
	public T get() throws IOException {
		if (this.buf == null || this.buf.remaining() < Integer.BYTES) {
			fillBuffer();
		}
		
		if (type.equals(Long.class)) {
			return (T) ((Long) buf.getLong()); 
		} else if (type.equals(Integer.class)) {
			return (T) ((Integer) buf.getInt());
		} else if (type.equals(Double.class)) {
			return (T) ((Double) buf.getDouble());
		} else if (type.equals(Float.class)) {
			return (T) ((Float) buf.getFloat());
		} else {
			int len = buf.getInt();
			byte[] b = new byte[len];
			buf.get(b);
			return (T) new String(b, UTF_8);
		}
	}
	
	@Override
	public int read() throws IOException {
		throw new IOException("Unsupported operation.");
	}

}
