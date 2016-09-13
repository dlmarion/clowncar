package io.github.dlmarion.clowncar;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public abstract class TestBase {
	
	protected static final ByteBuffer TIMES = ByteBuffer.allocateDirect(Long.BYTES*1440);
	protected static long TIMES_LENGTH;
	protected static final ByteBuffer FLAT = ByteBuffer.allocateDirect(Double.BYTES * 1440);
	protected static long FLAT_LENGTH;
	protected static final ByteBuffer RATE = ByteBuffer.allocateDirect(Double.BYTES * 1440);
	protected static long RATE_LENGTH;
	protected static final ByteBuffer WAVE = ByteBuffer.allocateDirect(Double.BYTES * 1440);
	protected static long WAVE_LENGTH;
	protected static final Random RAND = new Random(235235159511L);
	protected static final long NOW = System.currentTimeMillis();
	protected static final long BEGIN = NOW - 86400000;
	protected static final double C = RAND.nextDouble();
	protected static final double ZERO = 0.0D;
	protected static final double LOW = Math.abs(RAND.nextDouble()) * -1;
	protected static final double HIGH = RAND.nextDouble();

	static {
		long time = BEGIN;
		for (int i = 0; i < 1440; i++) {
			TIMES.putLong(time);
			time+=60000;
		}
		TIMES_LENGTH = TIMES.position();
		TIMES.position(0);
		
		for (int i = 0; i < 1440; i++) {
			FLAT.putDouble(C);
		}
		FLAT_LENGTH = FLAT.position();
		FLAT.position(0);
		
		for (int i = 0; i < 1440; i++) {
			RATE.putDouble(C + (125*i));
		}
		RATE_LENGTH = RATE.position();
		RATE.position(0);
		
		for (int i = 0; i < 1440/4; i+=4) {
			WAVE.putDouble(ZERO);
			WAVE.putDouble(HIGH);
			WAVE.putDouble(ZERO);
			WAVE.putDouble(LOW);
		}
		WAVE_LENGTH = WAVE.position();
		WAVE.position(0);
		
	}
	
	@Parameterized.Parameters
	public static Collection<Object[]> generateParameters() {
		Collection<Object[]> params = new ArrayList<>();
		for (String compressor : new String[]{BloscBase.BLOSCLZ, BloscBase.LZ4, BloscBase.LZ4HC, BloscBase.SNAPPY, BloscBase.ZLIB, BloscBase.ZSTD}) {
			for (int level : new int[]{1,2,3,4,5,6,7,8,9}) {
				for (int shuffle : new int[]{0,1,2}) {
					for (int threads : new int[] {1/*,2,3,4*/}) {
						params.add(new Object[]{compressor, level, shuffle, threads});
					}
				}
			}
			
		}
		return params;
	}
	
	private final Logger LOG = LoggerFactory.getLogger(this.getClass());
	private String compressor;
	private int level;
	private int shuffle;
	private int threads;

	public TestBase(String compressor, int level, int shuffle, int threads) {
		super();
		this.compressor = compressor;
		this.level = level;
		this.shuffle = shuffle;
		this.threads = threads;
	}
	
	public abstract int compress(int compressionLevel, int shuffleType,
			int typeSize, Buffer src, long srcLength, Buffer dest, 
			long destLength, String compressorName, int blockSize, int numThreads);
	
	public abstract int decompress(Buffer src, Buffer dest, long destSize, int numThreads);

	@Test
	public void testTimeCompression() throws Exception {
		long dstLength = TIMES_LENGTH + 16;
		ByteBuffer dest = ByteBuffer.allocateDirect((int)dstLength);
		ByteBuffer verify = ByteBuffer.allocateDirect((int)dstLength);
		
		int written = compress(this.level, this.shuffle, Long.BYTES, TIMES, TIMES_LENGTH, dest, 
				dstLength, this.compressor, (int)TIMES_LENGTH, this.threads);
		Assert.assertNotEquals("Error compressing data", -1, written);
		
		int read = decompress(dest, verify, dstLength, this.threads);
		Assert.assertNotEquals("Error decompressing data", -1, read);
		
		long time = BEGIN;
		for (int i = 0; i < 1440; i++) {
			Assert.assertEquals("failed to verify " + i  +"th long", time, verify.getLong());
			time+=60000;
		}
		
		LOG.info("Times [{},{},{},{}], original size: {}, compressed size: {}, Compression: {}%", 
				this.compressor, this.level, this.shuffle, this.threads, TIMES_LENGTH, written, (1.0D - (written * 1.0D / TIMES_LENGTH)) * 100);
	}

	@Test
	public void testFlatCompression() throws Exception {
		long dstLength = FLAT_LENGTH + 16;
		ByteBuffer dest = ByteBuffer.allocateDirect((int)dstLength);
		ByteBuffer verify = ByteBuffer.allocateDirect((int)dstLength);
		
		int written = compress(this.level, this.shuffle, Long.BYTES, FLAT, FLAT_LENGTH, dest, 
				dstLength, this.compressor, (int)FLAT_LENGTH, this.threads);
		Assert.assertNotEquals("Error compressing data", -1, written);
		
		int read = decompress(dest, verify, dstLength, this.threads);
		Assert.assertNotEquals("Error decompressing data", -1, read);
		
		for (int i = 0; i < 1440; i++) {
			Assert.assertEquals("failed to verify " + i  +"th long", C, verify.getDouble(), 0.0D);
		}
		
		LOG.info("Flat [{},{},{},{}], original size: {}, compressed size: {}, Compression: {}%", 
				this.compressor, this.level, this.shuffle, this.threads, FLAT_LENGTH, written, (1.0D - (written * 1.0D / FLAT_LENGTH)) * 100);
	}

	@Test
	public void testRateCompression() throws Exception {
		long dstLength = RATE_LENGTH + 16;
		ByteBuffer dest = ByteBuffer.allocateDirect((int)dstLength);
		ByteBuffer verify = ByteBuffer.allocateDirect((int)dstLength);
		
		int written = compress(this.level, this.shuffle, Long.BYTES, RATE, RATE_LENGTH, dest, 
				dstLength, this.compressor, (int)RATE_LENGTH, this.threads);
		Assert.assertNotEquals("Error compressing data", -1, written);
		
		int read = decompress(dest, verify, dstLength, this.threads);
		Assert.assertNotEquals("Error decompressing data", -1, read);
		
		for (int i = 0; i < 1440; i++) {
			Assert.assertEquals("failed to verify " + i  +"th long", C + (125*i), verify.getDouble(), 0.0D);
		}
		
		LOG.info("Rate [{},{},{},{}], original size: {}, compressed size: {}, Compression: {}%", 
				this.compressor, this.level, this.shuffle, this.threads, RATE_LENGTH, written, (1.0D - (written * 1.0D / RATE_LENGTH)) * 100);
	}

	@Test
	public void testWaveCompression() throws Exception {
		long dstLength = WAVE_LENGTH + 16;
		ByteBuffer dest = ByteBuffer.allocateDirect((int)dstLength);
		ByteBuffer verify = ByteBuffer.allocateDirect((int)dstLength);
		
		int written = compress(this.level, this.shuffle, Long.BYTES, WAVE, WAVE_LENGTH, dest, 
				dstLength, this.compressor, (int)RATE_LENGTH, this.threads);
		Assert.assertNotEquals("Error compressing data", -1, written);
		
		int read = decompress(dest, verify, dstLength, this.threads);
		Assert.assertNotEquals("Error decompressing data", -1, read);
		for (int i = 0; i < 1440/4; i+=4) {
			Assert.assertEquals("failed to verify " + i++  +"th long", ZERO, verify.getDouble(), 0.0D);
			Assert.assertEquals("failed to verify " + i++  +"th long", HIGH, verify.getDouble(), 0.0D);
			Assert.assertEquals("failed to verify " + i++  +"th long", ZERO, verify.getDouble(), 0.0D);
			Assert.assertEquals("failed to verify " + i++  +"th long", LOW, verify.getDouble(), 0.0D);
		}
		
		LOG.info("Wave [{},{},{},{}], original size: {}, compressed size: {}, Compression: {}%", 
				this.compressor, this.level, this.shuffle, this.threads, WAVE_LENGTH, written, (1.0D - (written * 1.0D / WAVE_LENGTH)) * 100);
	}

}
