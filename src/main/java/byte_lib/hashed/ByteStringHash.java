package byte_lib.hashed;

import byte_lib.string.ByteString;

public interface ByteStringHash {
    static ByteStringHash simple() {
        return new SimpleByteStringHash();
    }

    ByteStringHash n(int hashFunction);

    long hashCode(ByteString str, ByteString... other);

    long hashCode(ByteString str, long off, long len);

    class SimpleByteStringHash implements ByteStringHash {
        long multiplier = 31;

        @Override
        public ByteStringHash n(int hashFunction) {
            multiplier = hashFunction + 2;
            return this;
        }

        @Override
        public long hashCode(ByteString str, ByteString... other) {
            long hash = hashString(1L, str);
            for (ByteString s : other) {
                hash = hashString(hash, s);
            }
            return hash;
        }

        @Override
        public long hashCode(ByteString bs, long off, long len) {
            long result = 1;
            result = 31 * result + len;
            for (int i = 0; i < len; i++) {
                result = 31 * result + bs.byteAt(i + off);
            }
            return result;
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
