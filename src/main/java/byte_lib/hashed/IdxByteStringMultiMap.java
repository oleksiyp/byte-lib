package byte_lib.hashed;

import byte_lib.string.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static byte_lib.string.ByteString.*;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

public class IdxByteStringMultiMap {
    private final static Logger LOG = LoggerFactory.getLogger(IdxByteStringMultiMap.class);
    private final ByteStringHash hasher;

    private long [][]table;
    private int []tableLen;
    private int bucketsFilled;
    private int bits;

    private final ByteString chunk;
    private final ByteString itemSeparator;

    private final IdxMapper keyMapper;
    private final IdxMapper valueMapper;

    public IdxByteStringMultiMap(ByteString chunk,
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
        bits = Util.nBits(capacity);
        if (bits < 3) bits = 3;
        table = new long[1 << bits][];
        tableLen = new int[1 << bits];
        LOG.info("Rehash {} {}", bucketsFilled, table.length);
        bucketsFilled = 0;
    }
    
    public List<ByteString> get(Object key) {
        if (isEmpty()) {
            return Collections.emptyList();
        }
        ByteString keyStr = (ByteString) key;
        long keyLen = keyStr.length();
        long hash = hasher.hashCode(keyStr, 0L, keyLen);
        for (int n = 0; n < table.length; n++) {
            int item = openAddressItem(hash, n);

            long []entry = table[item];
            if (entry == null) {
                return Collections.emptyList();
            }
            if (isChunkKey(entry[0], keyStr)) {
                return stream(entry, 0, tableLen[item])
                        .mapToObj(this::value)
                        .collect(toList());
            }
        }
        return Collections.emptyList();
    }

    private boolean put0(long start, long end) {
        long keyIdx = keyMapper.map(chunk, start, end);
        long keyStart = idxStart(keyIdx);
        long keyLen = idxLen(keyIdx);
        long hash = hasher.hashCode(chunk, keyStart, keyLen);
        for (int n = 0; n < table.length; n++) {
            int item = openAddressItem(hash, n);

            long []entry = table[item];

            if (entry == null) {
                table[item] = new long[1];
                addTable(item, encodeIdx(start, end));
                bucketsFilled++;
                return true;
            }

            if (isChunkKey(entry[0], chunk, keyStart, keyLen)) {
                addTable(item, encodeIdx(start, end));
                bucketsFilled++;
                return true;
            }
        }
        return true;
    }

    private void addTable(int item, long v) {
        int len = tableLen[item];
        if (len == table[item].length) {
            table[item] = Arrays.copyOf(table[item], table[item].length * 2);
        }
        table[item][len] = v;
        tableLen[item]++;
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