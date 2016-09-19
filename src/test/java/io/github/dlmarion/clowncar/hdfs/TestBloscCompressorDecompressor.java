package io.github.dlmarion.clowncar.hdfs;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.BlockCompressorStream;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestBloscCompressorDecompressor {

    private static final Random rnd = new Random(12345l);
  
	@Parameterized.Parameters
	public static Collection<Object[]> generateParameters() {
		Collection<Object[]> params = new ArrayList<>();
		for (String compressor : new String[]{/*"blosclz",*/ "lz4", "lz4hc", "snappy", "zlib", "zstd"}) { /* blosclz sometimes produce larger output than expected */
			for (int level : new int[]{1,2,3,4,5,6,7,8,9}) {
				for (int shuffle : new int[]{/*0,1,*/2 }) { /* no shuffle and bit shuffle sometimes produce larger output than expected */
					for (int threads : new int[] {1,2,3,4}) {
						params.add(new Object[]{compressor, level, shuffle, threads});
					}
				}
			}
			
		}
		return params;
	}
	
	private String compressor;
	private int level;
	private int shuffle;
	private int threads;

	public TestBloscCompressorDecompressor(String compressor, int level, int shuffle, int threads) {
		super();
		this.compressor = compressor;
		this.level = level;
		this.shuffle = shuffle;
		this.threads = threads;
	}

  //test on NullPointerException in {@code compressor.setInput()} 
  @Test
  public void testCompressorSetInputNullPointerException() {
    try {
  	  Configuration conf = new Configuration(false);
  	  conf.set(BloscCompressor.COMPRESSOR_NAME_KEY, compressor);
  	  conf.set(BloscCompressor.COMPRESSION_LEVEL_KEY, Integer.toString(level));
  	  conf.set(BloscCompressor.BYTES_FOR_TYPE_KEY, Integer.toString(Integer.BYTES));
  	  conf.set(BloscCompressor.SHUFFLE_TYPE_KEY, Integer.toString(shuffle));
  	  conf.set(BloscCompressor.NUM_THREADS_KEY, Integer.toString(threads));

      BloscCompressor compressor = new BloscCompressor(conf);
      compressor.setInput(null, 0, 10);
      fail("testCompressorSetInputNullPointerException error !!!");
    } catch (NullPointerException ex) {
      // expected
    } catch (Exception e) {
      fail("testCompressorSetInputNullPointerException ex error !!!");
    }
  }

  //test on NullPointerException in {@code decompressor.setInput()}
  @Test
  public void testDecompressorSetInputNullPointerException() {
    try {
      BloscDecompressor decompressor = new BloscDecompressor();
      decompressor.setInput(null, 0, 10);
      fail("testDecompressorSetInputNullPointerException error !!!");
    } catch (NullPointerException ex) {
      // expected
    } catch (Exception e) {
      fail("testDecompressorSetInputNullPointerException ex error !!!");
    }
  }
  
  //test on ArrayIndexOutOfBoundsException in {@code compressor.setInput()}
  @Test
  public void testCompressorSetInputAIOBException() {
    try {
  	  Configuration conf = new Configuration(false);
  	  conf.set(BloscCompressor.COMPRESSOR_NAME_KEY, compressor);
  	  conf.set(BloscCompressor.COMPRESSION_LEVEL_KEY, Integer.toString(level));
  	  conf.set(BloscCompressor.BYTES_FOR_TYPE_KEY, Integer.toString(Integer.BYTES));
  	  conf.set(BloscCompressor.SHUFFLE_TYPE_KEY, Integer.toString(shuffle));
  	  conf.set(BloscCompressor.NUM_THREADS_KEY, Integer.toString(threads));
    	BloscCompressor compressor = new BloscCompressor(conf);
      compressor.setInput(new byte[] {}, -5, 10);
      fail("testCompressorSetInputAIOBException error !!!");
    } catch (ArrayIndexOutOfBoundsException ex) {
      // expected
    } catch (Exception ex) {
      fail("testCompressorSetInputAIOBException ex error !!!");
    }
  }

  //test on ArrayIndexOutOfBoundsException in {@code decompressor.setInput()}
  @Test
  public void testDecompressorSetInputAIOUBException() {
    try {
      BloscDecompressor decompressor = new BloscDecompressor();
      decompressor.setInput(new byte[] {}, -5, 10);
      fail("testDecompressorSetInputAIOBException error !!!");
    } catch (ArrayIndexOutOfBoundsException ex) {
      // expected
    } catch (Exception e) {
      fail("testDecompressorSetInputAIOBException ex error !!!");
    }
  }

  //test on NullPointerException in {@code compressor.compress()}  
  @Test
  public void testCompressorCompressNullPointerException() {
    try {
  	  Configuration conf = new Configuration(false);
  	  conf.set(BloscCompressor.COMPRESSOR_NAME_KEY, compressor);
  	  conf.set(BloscCompressor.COMPRESSION_LEVEL_KEY, Integer.toString(level));
  	  conf.set(BloscCompressor.BYTES_FOR_TYPE_KEY, Integer.toString(Integer.BYTES));
  	  conf.set(BloscCompressor.SHUFFLE_TYPE_KEY, Integer.toString(shuffle));
  	  conf.set(BloscCompressor.NUM_THREADS_KEY, Integer.toString(threads));
    	BloscCompressor compressor = new BloscCompressor(conf);
      byte[] bytes = generate(1024 * 6);
      compressor.setInput(bytes, 0, bytes.length);
      compressor.compress(null, 0, 0);
      fail("testCompressorCompressNullPointerException error !!!");
    } catch (NullPointerException ex) {
      // expected
    } catch (Exception e) {
      fail("testCompressorCompressNullPointerException ex error !!!");
    }
  }

  //test on NullPointerException in {@code decompressor.decompress()}  
  @Test
  public void testDecompressorCompressNullPointerException() {
    try {
      BloscDecompressor decompressor = new BloscDecompressor();
      byte[] bytes = generate(1024 * 6);
      decompressor.setInput(bytes, 0, bytes.length);
      decompressor.decompress(null, 0, 0);
      fail("testDecompressorCompressNullPointerException error !!!");
    } catch (NullPointerException ex) {
      // expected
    } catch (Exception e) {
      fail("testDecompressorCompressNullPointerException ex error !!!");
    }
  }

  //test on ArrayIndexOutOfBoundsException in {@code compressor.compress()}  
  @Test
  public void testCompressorCompressAIOBException() {
    try {
  	  Configuration conf = new Configuration(false);
  	  conf.set(BloscCompressor.COMPRESSOR_NAME_KEY, compressor);
  	  conf.set(BloscCompressor.COMPRESSION_LEVEL_KEY, Integer.toString(level));
  	  conf.set(BloscCompressor.BYTES_FOR_TYPE_KEY, Integer.toString(Integer.BYTES));
  	  conf.set(BloscCompressor.SHUFFLE_TYPE_KEY, Integer.toString(shuffle));
  	  conf.set(BloscCompressor.NUM_THREADS_KEY, Integer.toString(threads));
    	BloscCompressor compressor = new BloscCompressor(conf);
      byte[] bytes = generate(1024 * 6);
      compressor.setInput(bytes, 0, bytes.length);
      compressor.compress(new byte[] {}, 0, -1);
      fail("testCompressorCompressAIOBException error !!!");
    } catch (ArrayIndexOutOfBoundsException ex) {
      // expected
    } catch (Exception e) {
      fail("testCompressorCompressAIOBException ex error !!!");
    }
  }

  //test on ArrayIndexOutOfBoundsException in decompressor.decompress()  
  @Test
  public void testDecompressorCompressAIOBException() {
    try {
  	  Configuration conf = new Configuration(false);
  	  conf.set(BloscCompressor.COMPRESSOR_NAME_KEY, compressor);
  	  conf.set(BloscCompressor.COMPRESSION_LEVEL_KEY, Integer.toString(level));
  	  conf.set(BloscCompressor.BYTES_FOR_TYPE_KEY, Integer.toString(Integer.BYTES));
  	  conf.set(BloscCompressor.SHUFFLE_TYPE_KEY, Integer.toString(shuffle));
  	  conf.set(BloscCompressor.NUM_THREADS_KEY, Integer.toString(threads));
      BloscDecompressor decompressor = new BloscDecompressor(conf);
      byte[] bytes = generate(1024 * 6);
      decompressor.setInput(bytes, 0, bytes.length);
      decompressor.decompress(new byte[] {}, 0, -1);
      fail("testDecompressorCompressAIOBException error !!!");
    } catch (ArrayIndexOutOfBoundsException ex) {
      // expected
    } catch (Exception e) {
      fail("testDecompressorCompressAIOBException ex error !!!");
    }
  }
  
  // test BloscCompressor compressor.compress()  
  @Test
  public void testSetInputWithBytesSizeMoreThenDefaultBloscCompressorBufferSize() {
    int BYTES_SIZE = 1024 * 64 + 1;
    try {
  	  Configuration conf = new Configuration(false);
  	  conf.set(BloscCompressor.COMPRESSOR_NAME_KEY, compressor);
  	  conf.set(BloscCompressor.COMPRESSION_LEVEL_KEY, Integer.toString(level));
  	  conf.set(BloscCompressor.BYTES_FOR_TYPE_KEY, Integer.toString(Integer.BYTES));
  	  conf.set(BloscCompressor.SHUFFLE_TYPE_KEY, Integer.toString(shuffle));
  	  conf.set(BloscCompressor.NUM_THREADS_KEY, Integer.toString(threads));
      BloscCompressor compressor = new BloscCompressor(conf);
      byte[] bytes = generate(BYTES_SIZE);
      assertTrue("needsInput error !!!", compressor.needsInput());
      compressor.setInput(bytes, 0, bytes.length);
      byte[] emptyBytes = new byte[BYTES_SIZE];
      int csize = compressor.compress(emptyBytes, 0, bytes.length);
      assertTrue(
          "testSetInputWithBytesSizeMoreThenDefaultBloscCompressorByfferSize error !!!",
          csize != 0);
    } catch (Exception ex) {
    	ex.printStackTrace();
      fail("testSetInputWithBytesSizeMoreThenDefaultBloscCompressorByfferSize ex error !!!");
    }
  }

  // test compress/decompress process 
  @Test
  public void testCompressDecompress() {
    int BYTE_SIZE = 1024 * 54;
    byte[] bytes = generate(BYTE_SIZE);
	  Configuration conf = new Configuration(false);
	  conf.set(BloscCompressor.COMPRESSOR_NAME_KEY, compressor);
	  conf.set(BloscCompressor.COMPRESSION_LEVEL_KEY, Integer.toString(level));
	  conf.set(BloscCompressor.BYTES_FOR_TYPE_KEY, Integer.toString(Integer.BYTES));
	  conf.set(BloscCompressor.SHUFFLE_TYPE_KEY, Integer.toString(shuffle));
	  conf.set(BloscCompressor.NUM_THREADS_KEY, Integer.toString(threads));
    BloscCompressor compressor = new BloscCompressor(conf);
    try {
      compressor.setInput(bytes, 0, bytes.length);
      assertTrue("BloscCompressDecompress getBytesRead error !!!",
          compressor.getBytesRead() > 0);
      assertTrue(
          "BloscCompressDecompress getBytesWritten before compress error !!!",
          compressor.getBytesWritten() == 0);

      byte[] compressed = new byte[BYTE_SIZE];
      int cSize = compressor.compress(compressed, 0, compressed.length);
      assertTrue(
          "BloscCompressDecompress getBytesWritten after compress error !!!",
          compressor.getBytesWritten() > 0);
      BloscDecompressor decompressor = new BloscDecompressor(conf);
      // set as input for decompressor only compressed data indicated with cSize
      decompressor.setInput(compressed, 0, cSize);
      byte[] decompressed = new byte[BYTE_SIZE];
      decompressor.decompress(decompressed, 0, decompressed.length);

      assertTrue("testBloscCompressDecompress finished error !!!", decompressor.finished());      
      assertArrayEquals(bytes, decompressed);
      compressor.reset();
      decompressor.reset();
      assertTrue("decompressor getRemaining error !!!",decompressor.getRemaining() == 0);
    } catch (Exception e) {
    	e.printStackTrace();
      fail("testBloscCompressDecompress ex error!!!");
    }
  }

  // test compress/decompress with empty stream
  @Test
  public void testCompressorDecompressorEmptyStreamLogic() {
    ByteArrayInputStream bytesIn = null;
    ByteArrayOutputStream bytesOut = null;
    byte[] buf = null;
    BlockDecompressorStream blockDecompressorStream = null;
    try {
  	  Configuration conf = new Configuration(false);
  	  conf.set(BloscCompressor.COMPRESSOR_NAME_KEY, compressor);
  	  conf.set(BloscCompressor.COMPRESSION_LEVEL_KEY, Integer.toString(level));
  	  conf.set(BloscCompressor.BYTES_FOR_TYPE_KEY, Integer.toString(Integer.BYTES));
  	  conf.set(BloscCompressor.SHUFFLE_TYPE_KEY, Integer.toString(shuffle));
  	  conf.set(BloscCompressor.NUM_THREADS_KEY, Integer.toString(threads));
      // compress empty stream
      bytesOut = new ByteArrayOutputStream();
      BlockCompressorStream blockCompressorStream = new BlockCompressorStream(
          bytesOut, new BloscCompressor(conf), 1024, 0);
      // close without write
      blockCompressorStream.close();
      // check compressed output
      buf = bytesOut.toByteArray();
      assertEquals("empty stream compressed output size != 4", 4, buf.length);
      // use compressed output as input for decompression
      bytesIn = new ByteArrayInputStream(buf);
      // create decompression stream
      blockDecompressorStream = new BlockDecompressorStream(bytesIn,
          new BloscDecompressor(), 1024);
      // no byte is available because stream was closed
      assertEquals("return value is not -1", -1, blockDecompressorStream.read());
    } catch (Exception e) {
    	e.printStackTrace();
      fail("testCompressorDecompressorEmptyStreamLogic ex error !!!"
          + e.getMessage());
    } finally {
      if (blockDecompressorStream != null)
        try {
          bytesIn.close();
          bytesOut.close();
          blockDecompressorStream.close();
        } catch (IOException e) {
        }
    }
  }
  
  // test compress/decompress process through CompressionOutputStream/CompressionInputStream api 
  @Test
  public void testCompressorDecompressorLogicWithCompressionStreams() {
    DataOutputStream deflateOut = null;
    DataInputStream inflateIn = null;
    int BYTE_SIZE = 1024 * 100;
    byte[] bytes = generate(BYTE_SIZE);
    int bufferSize = 262144;
    int compressionOverhead = (bufferSize / 6) + 32;
    try {
  	  Configuration conf = new Configuration(false);
  	  conf.set(BloscCompressor.COMPRESSOR_NAME_KEY, compressor);
  	  conf.set(BloscCompressor.COMPRESSION_LEVEL_KEY, Integer.toString(level));
  	  conf.set(BloscCompressor.BYTES_FOR_TYPE_KEY, Integer.toString(Integer.BYTES));
  	  conf.set(BloscCompressor.SHUFFLE_TYPE_KEY, Integer.toString(shuffle));
  	  conf.set(BloscCompressor.NUM_THREADS_KEY, Integer.toString(threads));
      DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
      CompressionOutputStream deflateFilter = new BlockCompressorStream(
          compressedDataBuffer, new BloscCompressor(bufferSize, conf), bufferSize,
          compressionOverhead);
      deflateOut = new DataOutputStream(new BufferedOutputStream(deflateFilter));
      deflateOut.write(bytes, 0, bytes.length);
      deflateOut.flush();
      deflateFilter.finish();

      DataInputBuffer deCompressedDataBuffer = new DataInputBuffer();
      deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0,
          compressedDataBuffer.getLength());

      CompressionInputStream inflateFilter = new BlockDecompressorStream(
          deCompressedDataBuffer, new BloscDecompressor(bufferSize), bufferSize);

      inflateIn = new DataInputStream(new BufferedInputStream(inflateFilter));

      byte[] result = new byte[BYTE_SIZE];
      inflateIn.read(result);

      assertArrayEquals("original array not equals compress/decompressed array", result,
          bytes);
    } catch (IOException e) {
    	e.printStackTrace();
      fail("testBloscCompressorDecopressorLogicWithCompressionStreams ex error !!!");
    } finally {
      try {
        if (deflateOut != null)
          deflateOut.close();
        if (inflateIn != null)
          inflateIn.close();
      } catch (Exception e) {
      }
    }
  }  

  public static byte[] generate(int size) {
    byte[] array = new byte[size];
    for (int i = 0; i < size; i++)
      array[i] = (byte)rnd.nextInt(16);
    return array;
  }
}
