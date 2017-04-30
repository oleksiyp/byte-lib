package byte_lib;

import static byte_lib.ByteString.encodeIdx;
import static byte_lib.ByteString.idxLen;
import static byte_lib.ByteString.idxStart;

public class IdxByteStringMap {
    private final ByteStringHash hasher;

    private long []table;
    private int bucketsFilled;
    private int bits;

    private final ByteString chunk;
    private final ByteString itemSeparator;

    private final IdxMapper keyMapper;
    private final IdxMapper valueMapper;

    public IdxByteStringMap(ByteString chunk,
                            ByteString itemSeparator,
                            IdxMapper keyMapper,
                            IdxMapper valueMapper) {

        this.chunk = chunk;
        this.itemSeparator = itemSeparator;
        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;

        int records = chunk.howMuch(itemSeparator);

        allocateCapacity(records);

        hasher = ByteStringHash.simple();

        indexChunk();
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
                return value(entry - 1);
            }
        }
        return null;
    }

    private boolean put0(long start, long end) {
        long keyIdx = keyMapper.map(chunk, start, end);
        long keyStart = idxStart(keyIdx);
        long keyLen = idxLen(keyIdx);
        long hash = hasher.hashCode(chunk, keyStart, keyLen);
        for (int n = 0; n < table.length; n++) {
            int item = openAddressItem(hash, n);

            long entry = table[item];

            if (entry == 0) {
                table[item] = encodeIdx(start, end) + 1;
                bucketsFilled++;
                return true;
            }

            if (isChunkKey(entry - 1, chunk, keyStart, keyLen)) {
                table[item] = encodeIdx(start, end) + 1;
                return true;
            }
        }
        return true;
    }

    private boolean isChunkKey(long entryIdx, ByteString key2) {
        return isChunkKey(entryIdx, key2, 0, key2.length());
    }

    private boolean isChunkKey(long entryIdx, ByteString key2, long key2Start, long key2Len) {
        long entryStart = idxStart(entryIdx);
        long entryLen = idxLen(entryIdx);

        long keyIdx = keyMapper.map(chunk, entryStart, entryStart + entryLen);
        long key1Start = idxStart(keyIdx);
        long key1Len = idxLen(keyIdx);

        if (key1Len != key2Len) {
            return false;
        }

        for (long i = 0; i < key2Len; i++) {
            if (chunk.byteAt(key1Start + i) != key2.byteAt(key2Start + i)) {
                return false;
            }
        }

        return true;
    }

    private ByteString value(long entryIdx) {
        long entryStart = idxStart(entryIdx);
        long entryLen = idxLen(entryIdx);

        long idx = valueMapper.map(chunk, entryStart, entryStart + entryLen);
        long valueStart = idxStart(idx);
        long valueLen = idxLen(idx);

        return chunk.substring(valueStart, valueStart + valueLen);
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