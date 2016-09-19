package io.github.dlmarion.clowncar.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CompressorStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;
import org.apache.hadoop.io.compress.DirectDecompressionCodec;
import org.apache.hadoop.io.compress.DirectDecompressor;

public class BloscCodec implements Configurable, CompressionCodec, DirectDecompressionCodec {
	
	public static final String BUFFER_SIZE = "blosc.codec.stream.buffer.size";
	private static final int DEFAULT_BUFFER_SIZE = 64*1024;

	private Configuration conf;
	
	private int getBufferSize() {
		if (null == conf) {
			return DEFAULT_BUFFER_SIZE;
		}
		return conf.getInt(BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
	}
	
	@Override
	public Compressor createCompressor() {
		return new BloscCompressor(conf);
	}

	@Override
	public Decompressor createDecompressor() {
		return new BloscDecompressor(conf);
	}

	@Override
	public CompressionInputStream createInputStream(InputStream in) throws IOException {
		return createInputStream(in, new BloscDecompressor(conf));
	}

	@Override
	public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor) throws IOException {
		return new DecompressorStream(in, decompressor, getBufferSize());
	}

	@Override
	public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
		return createOutputStream(out, new BloscCompressor(conf));
	}

	@Override
	public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) throws IOException {
		return new CompressorStream(out, compressor, getBufferSize());
	}

	@Override
	public Class<? extends Compressor> getCompressorType() {
		return BloscCompressor.class;
	}

	@Override
	public Class<? extends Decompressor> getDecompressorType() {
		return BloscDecompressor.class;
	}

	@Override
	public String getDefaultExtension() {
		return ".blosc";
	}

	@Override
	public DirectDecompressor createDirectDecompressor() {
		return new BloscDecompressor(conf);
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

}
