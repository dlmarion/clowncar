package io.github.dlmarion.clowncar.io;

import io.github.dlmarion.clowncar.BloscCompressorType;
import io.github.dlmarion.clowncar.BloscShuffleType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;


public class StreamTest {

	private Random rand = new Random(2352514135L);
	
	@Test
	public void testLongs() throws Exception {
		List<Long> expected = new ArrayList<>();
		for (int i = 0; i < 1000; i++) {
			expected.add(Long.valueOf(i));
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream(Long.BYTES*500);
		try (BloscOutputStream<Long> blosc = new BloscOutputStream<>(baos, Long.BYTES*100, Long.class, BloscCompressorType.LZ4, 6, BloscShuffleType.BYTE_SHUFFLE, 2)) {
			for (Long l : expected) {
				blosc.write(l);
			}
		}
		byte[] compressedData = baos.toByteArray();
		ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
		try (BloscInputStream<Long> bloscIn = new BloscInputStream<>(bais, Long.class, 4)) {
			for (Long l : expected) {
				Long m = bloscIn.get();
				Assert.assertEquals(l, m);
			}
		}
	}

	@Test
	public void testDoubles() throws Exception {
		List<Double> expected = new ArrayList<>();
		for (int i = 0; i < 1000; i++) {
			expected.add(rand.nextDouble()*rand.nextLong());
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream(Double.BYTES*500);
		try (BloscOutputStream<Double> blosc = new BloscOutputStream<>(baos, Double.BYTES*100, Double.class, BloscCompressorType.LZ4, 6, BloscShuffleType.BYTE_SHUFFLE, 2)) {
			for (Double l : expected) {
				blosc.write(l);
			}
		}
		byte[] compressedData = baos.toByteArray();
		ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
		try (BloscInputStream<Double> bloscIn = new BloscInputStream<>(bais, Double.class, 4)) {
			for (Double l : expected) {
				Double m = bloscIn.get();
				Assert.assertEquals(l, m, 0.0D);
			}
		}
	}

	@Test
	public void testInts() throws Exception {
		List<Integer> expected = new ArrayList<>();
		for (int i = 0; i < 1000; i++) {
			expected.add(rand.nextInt());
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream(Integer.BYTES*500);
		try (BloscOutputStream<Integer> blosc = new BloscOutputStream<>(baos, Integer.BYTES*100, Integer.class, BloscCompressorType.LZ4, 6, BloscShuffleType.NO_SHUFFLE, 2)) {
			for (Integer l : expected) {
				blosc.write(l);
			}
		}
		byte[] compressedData = baos.toByteArray();
		ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
		try (BloscInputStream<Integer> bloscIn = new BloscInputStream<>(bais, Integer.class, 4)) {
			for (Integer l : expected) {
				Integer m = bloscIn.get();
				Assert.assertEquals(l, m);
			}
		}
	}

	@Test
	public void testFloats() throws Exception {
		List<Float> expected = new ArrayList<>();
		for (int i = 0; i < 1000; i++) {
			expected.add(rand.nextFloat() * rand.nextLong());
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream(Float.BYTES*500);
		try (BloscOutputStream<Float> blosc = new BloscOutputStream<>(baos, Float.BYTES*100, Float.class, BloscCompressorType.LZ4, 6, BloscShuffleType.NO_SHUFFLE, 2)) {
			for (Float l : expected) {
				blosc.write(l);
			}
		}
		byte[] compressedData = baos.toByteArray();
		ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
		try (BloscInputStream<Float> bloscIn = new BloscInputStream<>(bais, Float.class, 4)) {
			for (Float l : expected) {
				Float m = bloscIn.get();
				Assert.assertEquals(l, m);
			}
		}
	}
	
	@Test
	public void testStrings() throws Exception {
		List<String> expected = new ArrayList<>();
		for (int i = 0; i < 1000; i++) {
			expected.add(Long.toString(rand.nextLong()));
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream(20000);
		try (BloscOutputStream<String> blosc = new BloscOutputStream<>(baos, 1024, String.class, BloscCompressorType.ZLIB, 6, BloscShuffleType.NO_SHUFFLE, 2)) {
			for (String l : expected) {
				blosc.write(l);
			}
		}
		byte[] compressedData = baos.toByteArray();
		ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
		try (BloscInputStream<String> bloscIn = new BloscInputStream<>(bais, String.class, 4)) {
			for (String l : expected) {
				String m = bloscIn.get();
				Assert.assertEquals(l, m);
			}
		}
	}
	
}
