package byte_lib.hashed;

import byte_lib.string.ByteString;

import java.util.Arrays;

class BloomByteStringFilter implements ByteStringFilter {
    public static final int LONG_BITS_SIZE = 6;
    public static final int LONG_BITS_SIZE_MASK = (1 << 6) - 1;
    private long data[];
    private int nHashes;
    private final int mask;
    private final ByteStringHash hasher;
    private boolean empty;

    BloomByteStringFilter(int sz2degree, int nHashes) {
        this.nHashes = nHashes;
        if (sz2degree < LONG_BITS_SIZE) sz2degree = LONG_BITS_SIZE;
        int nLongs = 1 << (sz2degree - LONG_BITS_SIZE);
        data = new long[nLongs];
        mask = (1 << sz2degree) - 1;
        hasher = ByteStringHash.simple();
        empty = true;
    }

    @Override
    public boolean contains(ByteString str, ByteString... other) {
        boolean ret = true;
        for (int i = 0; i < nHashes; i++) {
            int hash = (int) (hasher.n(i).hashCode(str, other) & mask);
            ret &= getBit(hash >> LONG_BITS_SIZE, hash & LONG_BITS_SIZE_MASK);
        }
        return ret;
    }

    @Override
    public boolean add(ByteString str, ByteString... other) {
        boolean ret = true;
        for (int i = 0; i < nHashes; i++) {
            int hash = (int) (hasher.n(i).hashCode(str, other) & mask);
            ret &= setBit(hash >> LONG_BITS_SIZE, hash & LONG_BITS_SIZE_MASK);
        }
        return !ret;
    }

    private boolean setBit(int longIdx, int bit) {
        long msk = 1L << bit;
        long val = data[longIdx];
        data[longIdx] = val | msk;
        empty = false;
        return (val & msk) > 0;
    }

    private boolean getBit(int longIdx, int bit) {
        long msk = 1L << bit;
        long val = data[longIdx];
        return (val & msk) == msk;
    }

    @Override
    public boolean isEmpty() {
        return empty;
    }

    @Override
    public void clear() {
        Arrays.fill(data, 0);
        empty = true;
    }
}
