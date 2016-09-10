package io.github.dlmarion.clowncar;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

public class BloscLibraryTest {
	
	static {
		System.setProperty("jna.library.path",
				           "/home/dave/Software/Blosc/c-blosc-1.11.1/lib");
	}

	@Test
	public void testCompression() {
		ByteBuffer buf = ByteBuffer.allocate(Double.BYTES*1000);
		for (double d = 0.0D; d < 1000.0; d++) {
			buf.putDouble(d);	
		}
		byte[] src = new byte[buf.position()];
		System.out.println("original byte[] is " + src.length + " bytes");
		buf.position(0);
		buf.get(src);
		
		byte[] dest = new byte[src.length + 16];
		
		int written = BloscLibrary.compress(9, BloscLibrary.BIT_SHUFFLE, 
				Double.BYTES, src, dest, BloscLibrary.SNAPPY, src.length, 4);
		System.out.println("Written: " + written);
		
		byte[] src2 = new byte[src.length];
		int read = BloscLibrary.blosc_decompress_ctx(dest, src2, new SizeT(src2.length), 4);
		System.out.println("Read: " + read);
		ByteBuffer b = ByteBuffer.wrap(src2);
		for (double d = 0.0D; d < 1000.0; d++) {
			Assert.assertEquals(d, b.getDouble(), 0.0D);
		}
		
	}
}
