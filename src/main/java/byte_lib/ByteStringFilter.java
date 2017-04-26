package byte_lib;

public interface ByteStringFilter {
    boolean contains(ByteString str, ByteString... other);

    boolean add(ByteString str, ByteString... other);

    boolean isEmpty();

    void clear();

    static ByteStringFilter mem(int capacity) {
        return new MemTableByteStringFilter(capacity);
    }

    static ByteStringFilter mem() {
        return new MemTableByteStringFilter();
    }

    static ByteStringFilter bloom(int sz2degree, int nHashes) {
        return new BloomByteStringFilter(sz2degree, nHashes);
    }
}
