package io.github.dlmarion.clowncar;

public enum BloscCompressorType {

	BLOSCLZ("blosclz"),
	LZ4("lz4"),
	LZ4HC("lz4hc"),
	SNAPPY("snappy"),
	ZLIB("zlib"),
	ZSTD("zstd");
	
	private final String compressorName;
	
	private BloscCompressorType(String name) {
		this.compressorName = name;
	}
	
	public String getCompressorName() {
		return this.compressorName;
	}

	public static BloscCompressorType getCompressorType(String name) {
		switch(name) {
			case "blosclz": return BLOSCLZ;
			case "lz4" : return LZ4;
			case "lz4hc" : return LZ4HC;
			case "snappy" : return SNAPPY;
			case "zlib" : return ZLIB;
			case "zstd" : return ZSTD;
			default: throw new IllegalArgumentException("Unknown type");
		}
	}
}
