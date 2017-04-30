package byte_lib;

public class OneChunkByteStringMap {
    private final ByteStringHash hasher;

    private long []table;
    private int bucketsFilled;
    private int bits;

    private final ByteString chunk;
    private final ByteString itemSeparator;
    private final ByteString keyValueSeparator;

    public OneChunkByteStringMap(ByteString chunk,
                                 ByteString itemSeparator,
                                 ByteString keyValueSeparator) {

        this.chunk = chunk;
        this.itemSeparator = itemSeparator;
        this.keyValueSeparator = keyValueSeparator;

        int records = chunk.howMuch(itemSeparator);

        allocateCapacity(records);

        indexChunk();
        hasher = ByteStringHash.simple();
    }

    private void indexChunk() {
        chunk.iterateIdx(itemSeparator, this::put0);
    }

    private void allocateCapacity(int capacity) {
        capacity *= 4;
        bits = Bytes.nBits(capacity);
        if (bits < 3) bits = 3;
        table = new long[1 << bits];
        bucketsFilled = 0;
        System.out.println("Rehash " + bucketsFilled + " " + table.length);
    }

    public ByteString get(Object key) {
        ByteString keyStr = (ByteString) key;
        long keyLen = keyStr.length();
        long hash = hasher.hashCode(keyStr, 0L, keyLen);
        for (int n = 0; n < table.length; n++) {
            int item = openAddressItem(hash, n);

            long entry = table[item];
            if (entry == 0) {
                return null;
            }
            if (isChunkKey(entry - 1, keyStr)) {
                return value(entry - 1, keyLen);
            }
        }
        return null;
    }

    private boolean put0(long start, long end) {
        long keyLen = keyLen(start);
        long hash = hasher.hashCode(chunk, start, keyLen);
        for (int n = 0; n < table.length; n++) {
            int item = openAddressItem(hash, n);

            long entry = table[item];

            if (entry == 0) {
                table[item] = start + 1;
                bucketsFilled++;
                return true;
            }

            if (isChunkKey(entry - 1, chunk, start, keyLen)) {
                table[item] = start + 1;
                return true;
            }
        }
        return true;
    }

    private boolean isChunkKey(long key1, ByteString key2) {
        return isChunkKey(key1, key2, 0, key2.length());
    }

    private boolean isChunkKey(long key1, ByteString key2, long start, long keyLen) {
        if (chunk.length() - key1 < keyLen + keyValueSeparator.length()) {
            return false;
        }

        for (long i = 0; i < keyLen; i++) {
            if (chunk.byteAt(key1 + i) != key2.byteAt(start + i)) {
                return false;
            }
        }

        for (long i = 0; i < keyValueSeparator.length(); i++) {
            if (chunk.byteAt(key1 + i + keyLen) != keyValueSeparator.byteAt(i)) {
                return false;
            }
        }

        return true;
    }

    private long keyLen(long addr) {
        long idx = chunk.indexOf(keyValueSeparator, addr, chunk.length());
        if (idx == -1) idx = chunk.length();
        return idx - addr;
    }

    private ByteString value(long addr, long keyLen) {
        long off = addr + keyLen + itemSeparator.length();
        long idx = chunk.indexOf(itemSeparator, off, chunk.length());
        if (idx == -1) idx = chunk.length();
        return chunk.substring(off, idx);
    }

    private int openAddressItem(long hash, int nHash) {
        return (int) ((hash + nHash * nHash) & ((1 << bits) - 1));
    }

    public int size() {
        return bucketsFilled;
    }

    public boolean isEmpty() {
        return size() == 0;
    }
}