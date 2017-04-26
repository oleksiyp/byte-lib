package byte_lib;

public interface ByteStringHash {
    ByteStringHash n(int hashFunction);

    long getHash(ByteString str, ByteString... other);

    static ByteStringHash simple() {
        return new SimpleByteStringHash();
    }

    class SimpleByteStringHash implements ByteStringHash {
        long multiplier = 31;

        @Override
        public ByteStringHash n(int hashFunction) {
            multiplier = hashFunction + 2;
            return this;
        }

        @Override
        public long getHash(ByteString str, ByteString... other) {
            long hash = hashString(1L, str);
            for (ByteString s : other) {
                hash = hashString(hash, s);
            }
            return hash;
        }

        private long hashString(long hash, ByteString s) {
            hash = multiplier * hash + s.length();
            for (int i = 0; i < s.length(); i++) {
                hash = multiplier * hash + s.byteAt(i);
            }
            return hash;
        }
    }
}
