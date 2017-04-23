package byte_lib;

public class ByteStringFilter {
    private long []table;
    private int bucketsFilled;
    private int bits;

    public ByteStringFilter() {
        this(10);
    }

    public ByteStringFilter(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity");
        allocateCapacity(capacity);
    }

    private void allocateCapacity(int capacity) {
        capacity *= 4;
        bits = nBits(capacity);
        if (bits < 3) bits = 3;
        table = new long[1 << bits];
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

    public boolean contains(ByteString str, ByteString ...other) {
        long hash = longHash(str, other);

        for (int n = 0; n < table.length; n++) {

            int item = openAddressItem(hash, n);

            long entry = table[item];
            if (entry == 0) {
                return false;
            }
            if (entry == hash) {
                return true;
            }
        }
        return false;
    }

    public boolean add(ByteString str, ByteString ...other) {
        if (bucketsFilled << 2 > table.length) {
            rehash();
        }
        long hash = longHash(str, other);
        return add0(hash);
    }

    private boolean add0(long hash) {
        for (int n = 0; n < table.length; n++) {
            int item = openAddressItem(hash, n);

            long entry = table[item];

            if (entry == 0) {
                table[item] = hash;
                bucketsFilled++;
                return true;
            }

            if (entry == hash) {
                return false;
            }
        }
        return false;
    }


    private void rehash() {
        long[] oldTable = table;

        allocateCapacity(size());

        for (long val : oldTable) {
            if (val != 0) {
                add0(val);
            }
        }
    }

    private long longHash(ByteString str, ByteString[] other) {
        long hash = str.longHash();
        for (ByteString s : other) {
            hash = s.longHash(hash);
        }
        return hash;
    }

    private int openAddressItem(long hash, int nHash) {
        return (int) (hash + nHash * nHash) & ((1 << bits) - 1);
    }

    public int size() {
        return bucketsFilled;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public void clear() {
        allocateCapacity(10);
    }

}