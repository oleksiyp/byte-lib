package byte_lib.hashed;

import byte_lib.string.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static byte_lib.io.ByteFiles.*;
import static byte_lib.string.ByteString.*;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

public class FileIdxByteStringMultiMap {
    private final static Logger LOG = LoggerFactory.getLogger(FileIdxByteStringMultiMap.class);
    private final ByteStringHash hasher;

    private ByteString content;

    private long [][]table;
    private int []tableLen;
    private int bucketsFilled;
    private int bits;

    private File file;

    private final IdxMapper keyMapper;
    private final IdxMapper valueMapper;
    private final RandomAccessFile randomAccessFile;
    private final File idxFile;

    public FileIdxByteStringMultiMap(File file,
                                     ByteString itemSeprartor,
                                     IdxMapper keyMapper,
                                     IdxMapper valueMapper) {

        this.file = file;

        if (file.getName().endsWith(".snappy")) {
            File newFile = new File(nonArchivedName(file.getPath()));

            if (!newFile.isFile()) {
                LOG.info("Unpacking " + file);
                inputStream(file).writeAll(printStream(newFile));
            }
            this.file = newFile;
        }

        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;

        hasher = ByteStringHash.simple();

        idxFile = new File(this.file.getPath() + ".idx.snappy");
        if (!idxFile.isFile()) {
            try (ByteString content = readAll(file)) {
                this.content = content;
                int records = content.howMuch(itemSeprartor);
                allocateCapacity(records);
                this.content.iterateIdx(itemSeprartor, this::put0);
                writeIdx();
                this.content = null;
            }
        } else {
            readIdx();
        }

        try {
            randomAccessFile = new RandomAccessFile(this.file, "r");
        } catch (FileNotFoundException e) {
            throw new IOError(e);
        }

    }

    private void readIdx() {
        LOG.info("Reading " + idxFile);
        try (DataInputStream in =
                     new DataInputStream(inputStream(idxFile))) {
            table = new long[in.readInt()][];
            tableLen = new int[table.length];
            for (int i = 0; i < table.length; i++) {
                tableLen[i] = in.readInt();
                table[i] = new long[Util.nBits(tableLen[i])];
                for (int j = 0; j < tableLen[i]; j++) {
                    table[i][j] = in.readLong();
                }
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    private void writeIdx() {
        LOG.info("Writing " + idxFile);
        try (DataOutputStream out =
                     new DataOutputStream(printStream(idxFile))) {
            out.writeInt(table.length);
            for (int i = 0; i < table.length; i++) {
                out.writeInt(tableLen[i]);
                for (int j = 0; j < tableLen[i]; j++) {
                    out.writeLong(table[i][j]);
                }
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
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
        long keyIdx = keyMapper.map(content, start, end);
        long keyStart = idxStart(keyIdx);
        long keyLen = idxLen(keyIdx);
        long hash = hasher.hashCode(content, keyStart, keyLen);
        for (int n = 0; n < table.length; n++) {
            int item = openAddressItem(hash, n);

            long []entry = table[item];

            if (entry == null) {
                table[item] = new long[1];
                addTable(item, encodeIdx(start, end));
                bucketsFilled++;
                return true;
            }

            if (isChunkKey(entry[0], content, keyStart, keyLen)) {
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

    private boolean isChunkKey(long entryIdx, ByteString key2Entry, long key2Start, long key2Len) {
        long entryStart = idxStart(entryIdx);
        long entryLen = idxLen(entryIdx);

        ByteString key1Entry;
        long keyIdx;
        if (content == null) {
            key1Entry = seekAndRead(entryIdx);
            keyIdx = keyMapper.map(key1Entry, 0, key1Entry.length());
        } else {
            key1Entry = content;
            keyIdx = keyMapper.map(content, entryStart, entryStart + entryLen);
        }

        long key1Start = idxStart(keyIdx);
        long key1Len = idxLen(keyIdx);

        if (key1Len != key2Len) {
            return false;
        }

        for (long i = 0; i < key2Len; i++) {
            if (key1Entry.byteAt(key1Start + i) != key2Entry.byteAt(key2Start + i)) {
                return false;
            }
        }

        return true;
    }

    private ByteString seekAndRead(long entryIdx) {
        long entryStart = idxStart(entryIdx);
        long entryLen = idxLen(entryIdx);

        try {
            randomAccessFile.seek(entryStart);
            byte []entry = new byte[(int) entryLen];
            randomAccessFile.read(entry);
            return ba(entry);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    private ByteString value(long entryIdx) {
        ByteString entry = seekAndRead(entryIdx);

        long idx = valueMapper.map(entry, 0, entry.length());
        long valueStart = idxStart(idx);
        long valueLen = idxLen(idx);

        return entry.substring(valueStart, valueStart + valueLen);
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