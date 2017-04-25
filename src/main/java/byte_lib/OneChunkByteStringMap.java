package byte_lib;

public class OneChunkByteStringMap {
    private int []table;
    private int bucketsFilled;
    private int bits;
    private ByteString chunk;
    private ByteString itemSeparator;
    private ByteString keyValueSeparator;

    public OneChunkByteStringMap(ByteString chunk,
                                 ByteString itemSeparator,
                                 ByteString keyValueSeparator) {

        this.chunk = chunk;
        this.itemSeparator = itemSeparator;
        this.keyValueSeparator = keyValueSeparator;

        int []nRecords = new int[1];
        chunk.splitIterateIdx(itemSeparator, (s, e) -> nRecords[0]++);

        allocateCapacity(nRecords[0]);

        indexChunk();
    }

    private void indexChunk() {
        chunk.splitIterateIdx(itemSeparator, this::put0);
    }

    private void allocateCapacity(int capacity) {
        capacity *= 4;
        bits = nBits(capacity);
        if (bits < 3) bits = 3;
        table = new int[1 << bits];
        bucketsFilled = 0;
        System.out.println("Rehash " + bucketsFilled + " " + table.length);
    }

    private int nBits(int capacity) {
        int bits = 0;
        while (capacity > 0) {
            bits++;
            capacity >>= 1;
        }
        return bits;
    }

    public ByteString get(Object key) {
        ByteString keyStr = (ByteString) key;
        int keyLen = keyStr.length();
        int hash = hashCode(keyStr, 0, keyLen);
        for (int n = 0; n < table.length; n++) {
            int item = openAddressItem(hash, n);

            int entry = table[item];
            if (entry == 0) {
                return null;
            }
            if (isChunkKey(entry - 1, keyStr)) {
                return value(entry - 1, keyLen);
            }
        }
        return null;
    }

    private void put0(int start, int end) {
        int keyLen = keyLen(start);
        int hash = hashCode(chunk, start, keyLen);
        for (int n = 0; n < table.length; n++) {
            int item = openAddressItem(hash, n);

            int entry = table[item];

            if (entry == 0) {
                table[item] = start + 1;
                bucketsFilled++;
                return;
            }

            if (isChunkKey(entry - 1, chunk, start, keyLen)) {
                table[item] = start + 1;
                return;
            }
        }
    }

    private int hashCode(ByteString bs, int start, int len) {
        int result = 1;
        result = 31 * result + len;
        for (int i = 0; i < len; i++) {
            result = 31 * result + bs.byteAt(i + start);
        }
        return result;
    }

    private boolean isChunkKey(int key1, ByteString key2) {
        return isChunkKey(key1, key2, 0, key2.length());
    }

    private boolean isChunkKey(int key1, ByteString key2, int start, int keyLen) {
        if (chunk.length() - key1 < keyLen + keyValueSeparator.length()) {
            return false;
        }

        for (int i = 0; i < keyLen; i++) {
            if (chunk.byteAt(key1 + i) != key2.byteAt(start + i)) {
                return false;
            }
        }

        for (int i = 0; i < keyValueSeparator.length(); i++) {
            if (chunk.byteAt(key1 + i + keyLen) != keyValueSeparator.byteAt(i)) {
                return false;
            }
        }

        return true;
    }

    private int keyLen(int addr) {
        int idx = chunk.indexOf(keyValueSeparator, addr);
        if (idx == -1) idx = chunk.length();
        return idx - addr;
    }

    private ByteString value(int addr, int keyLen) {
        int off = addr + keyLen + itemSeparator.length();
        int idx = chunk.indexOf(itemSeparator, off);
        if (idx == -1) idx = chunk.length();
        return chunk.substring(off, idx);
    }

    private int openAddressItem(int hash, int nHash) {
        return (hash + nHash * nHash) & ((1 << bits) - 1);
    }

    public int size() {
        return bucketsFilled;
    }

    public boolean isEmpty() {
        return size() == 0;
    }
}