package byte_lib.hashed;

import byte_lib.string.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public class ByteStringMap<T> implements Map<ByteString, T> {
    private static final Logger LOG = LoggerFactory.getLogger(ByteStringMap.class);

    private HashEntry<T> []table;
    private int bucketsFilled;
    private int bucketsRemoved;
    private int bits;

    public ByteStringMap() {
        this(10);
    }

    public ByteStringMap(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity");
        allocateCapacity(capacity);
    }

    private void allocateCapacity(int capacity) {
        capacity *= 4;
        bits = Util.nBits(capacity);
        if (bits < 3) bits = 3;
        table = new HashEntry[1 << bits];
        LOG.debug("Rehash {} {} {}", bucketsFilled, bucketsRemoved, table.length);
        bucketsFilled = 0;
        bucketsRemoved = 0;
    }

    @Override
    public T get(Object key) {
        for (int n = 0; n < table.length; n++) {
            int item = openAddressItem((ByteString) key, n);

            HashEntry entry = table[item];
            if (entry == null) {
                return null;
            }
            if (entry.getKey().equals(key)) {
                return (T) entry.value;
            }
        }
        return null;
    }

    @Override
    public T put(ByteString str, T value) {
        if (bucketsRemoved << 2 > bucketsFilled) {
            rehash();
        } else if (bucketsFilled << 2 > table.length) {
            rehash();
        }
        return put0(str, value);
    }


    @Override
    public T remove(Object key) {
        return put((ByteString) key, null);
    }

    private T put0(ByteString str, T val) {
        for (int n = 0; n < table.length; n++) {
            int item = openAddressItem(str, n);

            HashEntry<T> entry = table[item];

            if (entry == null) {
                if (val != null) {
                    table[item] = new HashEntry<>(str, val);
                    bucketsFilled++;
                }
                return null;
            }

            if (entry.key.equals(str)) {
                T oldVal = entry.setValue(val);
                if (val == null ^ oldVal == null) {
                    if (val == null) {
                        bucketsRemoved++;
                    } else {
                        bucketsRemoved--;
                    }
                }
                return oldVal;
            }
        }
        return null;
    }

    private T put0(Entry<ByteString, T> entry) {
        return put0(entry.getKey(), entry.getValue());
    }

    private void rehash() {
        HashEntry<T>[] oldTable = table;

        allocateCapacity(size());

        Stream.of(oldTable)
                .filter(ByteStringMap::entryFilled)
                .forEach(this::put0);
    }

    private int openAddressItem(ByteString str, int nHash) {
        return (str.hashCode() + nHash * nHash) & ((1 << bits) - 1);
    }

    @Override
    public int size() {
        return bucketsFilled - bucketsRemoved;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        return entryStream()
                .map(HashEntry::getValue)
                .anyMatch(value::equals);
    }

    @Override
    public void putAll(Map<? extends ByteString, ? extends T> m) {
        m.forEach(this::put);
    }

    @Override
    public void clear() {
        allocateCapacity(10);
    }

    @Override
    public Set<ByteString> keySet() {
        return new AbstractSet<ByteString>() {
            @Override
            public Iterator<ByteString> iterator() {
                return entryStream()
                        .map(HashEntry::getKey)
                        .iterator();
            }

            @Override
            public int size() {
                return ByteStringMap.this.size();
            }
        };
    }

    @Override
    public String toString() {
        return "{" +
                entryStream()
                        .map(HashEntry::toString)
                        .collect(joining(", ")) +
                "}";
    }

    @Override
    public Collection<T> values() {
        return new AbstractCollection<T>() {
            @Override
            public Iterator<T> iterator() {
                return entryStream()
                        .map(HashEntry::getValue)
                        .iterator();
            }

            @Override
            public int size() {
                return ByteStringMap.this.size();
            }
        };
    }

    @Override
    public Set<Entry<ByteString, T>> entrySet() {
        return new AbstractSet<Entry<ByteString, T>>() {
            @Override
            public Iterator<Entry<ByteString, T>> iterator() {
                return entryStream()
                        .map(e -> (Entry<ByteString, T>)e)
                        .iterator();
            }

            @Override
            public int size() {
                return ByteStringMap.this.size();
            }
        };
    }

    private Stream<HashEntry<T>> entryStream() {
        return Stream.of(table)
                .filter(ByteStringMap::entryFilled);
    }

    private static boolean entryFilled(HashEntry<?> obj) {
        return Objects.nonNull(obj) && Objects.nonNull(obj.value);
    }

    static class HashEntry<T> implements Entry<ByteString, T> {
        final ByteString key;
        T value;

        HashEntry(ByteString key, T value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return getKey() + "=" + getValue();
        }

        @Override
        public ByteString getKey() {
            return key;
        }

        @Override
        public T getValue() {
            return value;
        }

        @Override
        public T setValue(T value) {
            T ret = this.value;
            this.value = value;
            return ret;
        }
    }

}