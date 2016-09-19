package io.github.dlmarion.clowncar;

public enum BloscShuffleType {
	
	NO_SHUFFLE(0), BYTE_SHUFFLE(1), BIT_SHUFFLE(2);
	
	private int shuffle;
	
	private BloscShuffleType(int shuffle) {
		this.shuffle = shuffle;
	}
	
	public int getShuffleType() {
		return this.shuffle;
	}
	
	public static BloscShuffleType getShuffleType(int i) {
		switch (i) {
			case 0:
				return NO_SHUFFLE;
			case 1:
				return BYTE_SHUFFLE;
			case 2:
				return BIT_SHUFFLE;
			default:
				throw new IllegalArgumentException();
		 
		}
	}

}
