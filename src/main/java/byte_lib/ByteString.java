package byte_lib;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static byte_lib.ByteStringInputStream.file;
import static java.lang.String.format;

public class ByteString implements Comparable<ByteString> {
    public static final ByteString EMPTY = new ByteString(ByteBuf.allocate(0));
    public static final ByteString SEPARATOR = bs(" ");
    public static final ByteString NEW_LINE = bs("\n");

    private final ByteBuf buffer;

    public ByteString(ByteBuf buffer) {
        this.buffer = buffer;
    }

    public static ByteString bb(ByteBuffer buf) {
        return new ByteString(ByteBuf.wrap(buf));
    }

    public static ByteString bb(ByteBuf buf) {
        return new ByteString(buf);
    }

    public static ByteString bs(String s) {
        return bs(s, Charset.defaultCharset());
    }

    public static ByteString bs(String s, Charset charset) {
        byte[] buf = s.getBytes(charset);
        ByteBuffer buffer = ByteBuffer.allocateDirect(buf.length);
        buffer.put(buf).flip();
        return new ByteString(ByteBuf.wrap(buffer));
    }

    public static ByteString ba(byte[] buffer, int offset, int length) {
        return new ByteString(ByteBuf.wrap(ByteBuffer.wrap(buffer, offset, length)));
    }

    public ByteString cut(ByteString start, ByteString end) {
        long sIdx = 0;
        if (startsWith(start)) {
            sIdx += start.length();
        } else {
            return null;
        }

        long eIdx = length();
        if (endsWith(end)) {
            eIdx -= end.length();
        } else {
            return null;
        }

        return substring(sIdx, eIdx);
    }

    public long length() {
        return buffer.limit() - buffer.position();
    }

    public ByteString copyOf() {
        ByteBuf buf = ByteBuf.wrap(ByteBuffer.allocate((int) length()));
        long pos = buffer.position();
        long end = buffer.limit();
        buf.put(buffer);
        buffer.position(pos).limit(end);
        buf.position(0).limit(length());
        return new ByteString(buf);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ByteString that = (ByteString) o;

        if (length() != that.length()) {
            return false;
        }

        for (int i = 0; i < length(); i++) {
            if (byteAt(i) != that.byteAt(i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        long result = 1;
        result = 31 * result + length();
        for (int i = 0; i < length(); i++) {
            result = 31 * result + byteAt(i);
        }
        return (int) result;
    }


    @Override
    public String toString() {
        long len = Math.min(Integer.MAX_VALUE, length());
        byte buf[] = new byte[(int) len];
        long pos = buffer.position();
        long end = buffer.limit();
        buffer.get(buf);
        buffer.position(pos).limit(end);
        return new String(buf, 0, buf.length);
    }

    public void writeTo(PrintStream out) {
        for (long i = 0; i < length(); i++) {
            out.write(byteAt(i));
        }
    }

    public byte byteAt(long index) {
        return buffer.get(index + buffer.position());
    }

    public long lastIndexOf(byte ch) {
        for (long i = length() - 1; i >= 0; i--) {
            if (byteAt(i) == ch) {
                return i;
            }
        }
        return -1;
    }

    public ByteString substring(long from, long to) {
        long pos = buffer.position();
        ByteBuf buf = buffer.duplicate();
        buf.position(pos + from).limit(pos + to);
        return new ByteString(buf);
    }

    public boolean isEmpty() {
        return length() == 0;
    }

    public ByteString trim() {
        long from = 0;
        long to = length();
        while (isSpaceByte(byteAt(from)) && from < to)
            from++;
        while (isSpaceByte(byteAt(to - 1)) && from < to)
            to--;
        return substring(from, to);
    }

    public static boolean isSpaceByte(byte b) {
        return b == ' ' || b == '\n' || b == '\t' || b == '\r';
    }

    public boolean startsWith(ByteString string) {
        if (string.length() > length()) {
            return false;
        }
        for (long i = 0; i < string.length(); i++) {
            if (string.byteAt(i) != byteAt(i)) {
                return false;
            }
        }
        return true;
    }

    public boolean endsWith(ByteString string) {
        if (string.length() > length()) {
            return false;
        }
        long off = length() - string.length();
        for (long i = 0; i < string.length(); i++) {
            if (string.byteAt(i) != byteAt(off + i)) {
                return false;
            }
        }
        return true;
    }

    public long indexOf(byte ch) {
        return indexOf(ch, 0);
    }

    public long indexOf(ByteString str, long off) {
        if (str.length() == 1) {
            return indexOf(str.byteAt(0), off);
        }

        long strLen = str.length();
        long wholeLen = length() - strLen;
        for (long i = off; i <= wholeLen; i++) {
            boolean found = true;
            for (long j = 0; j < strLen; j++) {
                if (byteAt(i + j) != str.byteAt(j)) {
                    found = false;
                    break;
                }
            }
            if (found) {
                return i;
            }
        }
        return -1;
    }

    public long indexOf(byte ch, long off) {
        long wholeLen = length();
        for (long i = off; i < wholeLen; i++) {
            if (byteAt(i) == ch) {
                return i;
            }
        }
        return -1;
    }

    public ByteString substring(int start) {
        return substring(start, length());
    }

    public ByteString[] split(ByteString separator) {
        int n = 10;
        if (length() > 1024 * 512) {
            n = howMuch(separator);
        }
        List<ByteString> arr = new ArrayList<>(n);
        iterate(separator, arr::add);

        return arr.toArray(new ByteString[arr.size()]);
    }

    public long []splitIdx(ByteString separator) {
        int n = howMuch(separator);
        long[] arr = new long[n];
        int it[] = new int[1];
        iterateIdx(separator, (start, end) -> {
            arr[it[0]++] = encodeIdx(start, end);
            return true;
        });
        return arr;
    }

    public static long idxStart(long encoded) {
        return encoded >> 24;
    }

    public static long idxEnd(long encoded) {
        long start = encoded >> 24;
        long len = encoded & ((1 << 24) - 1);
        return start + len;
    }

    public static long idxLen(long encoded) {
        return encoded & ((1 << 24) - 1);
    }

    public static long encodeIdx(long start, long end) {
        long len = end - start;
        return (start << 24) | (len & ((1 << 24) - 1));
    }

    public void iterate(ByteString str, Consumer<ByteString> it) {
        iterateIdx(str, (start, end) -> {
            it.accept(substring(start, end));
            return true;
        });
    }

    public void iterateIdx(ByteString separator, SubstringIterator it) {
        if (separator.length() == 1) {
            byte sep = separator.byteAt(0);
            iterateIdx(sep, it);
            return;
        }
        long start = 0;
        while (start < length()) {
            long idx = indexOf(separator, start);

            if (idx == -1) {
                if (start < length()) {
                    it.substring(start, length());
                }
                break;
            }
            if (start < idx && !it.substring(start, idx)) {
                return;
            }
            start = idx + separator.length();
        }
    }

    private void iterateIdx(byte sep, SubstringIterator it) {
        long start = 0;
        for (long i = 0; i < length(); i++) {
            if (byteAt(i) == sep) {
                if (start < i && !it.substring(start, i)) {
                    return;
                }
                start = i + 1;
            }
        }
        if (start < length()) {
            it.substring(start, length());
        }
    }

    public ByteString append(ByteString otherStr) {
        ByteBuf buf = ByteBuf.wrap(ByteBuffer.allocate((int) (length() + otherStr.length())));
        long pos = buffer.position();
        long end = buffer.limit();

        long otherPos = otherStr.buffer.position();
        long otherEnd = otherStr.buffer.limit();

        buf.put(buffer).put(otherStr.buffer);

        buffer.position(pos).limit(end);
        otherStr.buffer.position(otherPos).limit(otherEnd);

        buf.position(0).limit(length() + otherStr.length());

        return new ByteString(buf);
    }

    @Override
    public int compareTo(ByteString o) {
        for (int i = 0; i < length() && i < o.length(); i++) {
            if (byteAt(i) < o.byteAt(i)) {
                return -1;
            } else if (byteAt(i) > o.byteAt(i)) {
                return 1;
            }
        }

        if (length() < o.length()) {
            return -1;
        } else if (length() > o.length()) {
            return 1;
        }

        return 0;
    }

    public long toLong() {
        long r = 0;
        int ptr = 0;

        int sign = 1;
        while (ptr < length() && (byteAt(ptr) == '-' || byteAt(ptr) == '+')) {
            if (byteAt(ptr) == '-') sign *= -1;
            ptr++;
        }

        while (ptr < length() && '0' <= byteAt(ptr) && byteAt(ptr) <= '9') {
            r *= 10;
            r += byteAt(ptr) - '0';
            ptr++;
        }
        return r * sign;
    }

    public int toInt() {
        return (int) toLong();
    }

    public ByteString fields(ByteString separator, int start, int end) {
        if (start > end) throw new IllegalArgumentException("start");
        long []r = new long[] { 0,  0, length() };
        iterateIdx(separator, (s, e) -> {
            if (r[0] == start) r[1] = s;
            if (r[0] == end) r[2] = e;
            r[0]++;
            return true;
        });
        return substring(r[1], r[2]);
    }

    public ByteString fields(int start, int end) {
        return fields(SEPARATOR, start, end);
    }

    public ByteString field(ByteString separator, int i) {
        return fields(separator, i, i);
    }

    public ByteString field(int i) {
        return fields(SEPARATOR, i, i);
    }

    public ByteString firstField() {
        return fields(0, 0);
    }

    public ByteString firstTwoFields() {
        return fields(0, 1);
    }

    public ByteString firstThreeFields() {
        return fields(0, 2);
    }

    public static ByteString load(String path) throws IOException {
        return load(new File(path), null);
    }

    public static ByteString load(String path, Progress progress) throws IOException {
        return load(new File(path), progress);
    }

    public static ByteString load(File file, Progress progress) throws IOException {
        String name = file.getName();
        progress = Progress.voidIfNull(progress);

        progress.message(format("Counting size of '%s'", name));
        long nBytes;
        try (ByteStringInputStream in = file(file)) {
            nBytes = in.countBytes();
        }

        progress.message(format("Allocating %s for '%s'", Bytes.sizeToString(nBytes), name));
        try (ByteStringInputStream in = file(file)){
            ByteBuf buf = in.readAll(nBytes, progress);
            return ByteString.bb(buf);
        } finally {
            progress.message(format("Done reading '%s'!", name));
        }
    }

    public int howMuch(ByteString str) {
        int []n = new int[1];
        iterateIdx(str, (s, e) -> {
            n[0]++;
            return true;
        });
        return n[0];
    }

    public int compareByIdx(long idx1, long idx2) {
        if (idx1 == idx2) {
            return 0;
        }
        long ptr1 = idxStart(idx1);
        long ptr2 = idxStart(idx2);

        long len1 = idxLen(idx1);
        long len2 = idxLen(idx2);

        for (int i = 0; i < len1 && i < len2; i++) {
            byte b1 = byteAt(ptr1++);
            byte b2 = byteAt(ptr2++);

            if (b1 < b2) {
                return -1;
            } else if (b1 > b2) {
                return 1;
            }
        }

        if (len1 < len2) {
            return -1;
        } else if (len1 > len2) {
            return 1;
        }

        return 0;
    }

    public void writeToByIdx(long idx, PrintStream out) {
        long start = idxStart(idx);
        long len = idxLen(idx);
        for (long i = 0; i < len; i++) {
            out.write(byteAt(i + start));
        }
    }
}