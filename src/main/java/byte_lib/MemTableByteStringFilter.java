package byte_lib;

class MemTableByteStringFilter implements ByteStringFilter {
    private final ByteStringHash hasher;
    private long []table;
    private int bucketsFilled;
    private int bits;

    MemTableByteStringFilter() {
        this(10);
    }

    MemTableByteStringFilter(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity");
        allocateCapacity(capacity);
        hasher = ByteStringHash.simple();
    }

    private void allocateCapacity(int capacity) {
        capacity *= 2;
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

    @Override
    public boolean contains(ByteString str, ByteString... other) {
        long hash = hasher.getHash(str, other);

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

    @Override
    public boolean add(ByteString str, ByteString... other) {
        if (bucketsFilled << 2 > table.length) {
            rehash();
        }
        long hash = hasher.getHash(str, other);
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

    private int openAddressItem(long hash, int nHash) {
        return (int) (hash + nHash * nHash) & ((1 << bits) - 1);
    }

    public int size() {
        return bucketsFilled;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public void clear() {
        allocateCapacity(10);
    }

}