package byte_lib.string;

import byte_lib.string.buf.ByteBuf;

import java.io.IOError;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static byte_lib.string.buf.ByteBuf.wrap;

public class ByteString implements Comparable<ByteString> {
    public static final ByteString EMPTY = bs("");
    public static final ByteString SEPARATOR = bs(" ");
    public static final ByteString NEW_LINE = bs("\n");

    public static final int EXCHANGE_BUF_SIZE = 64 * 1024;
    public static final int SMALL_WRITE_TO_THRESHOLD = 20;

    private final ByteBuf buffer;

    private ByteString(ByteBuf buffer) {
        this.buffer = buffer;
    }

    public static ByteString bb(ByteBuffer buf) {
        return new ByteString(wrap(buf));
    }

    public static ByteString bb(ByteBuf buf) {
        return new ByteString(buf);
    }

    public static ByteString bs(String s) {
        return bs(s, Charset.defaultCharset());
    }

    public static ByteString bs(String s, Charset charset) {
        return ba(s.getBytes(charset));
    }

    public static ByteString ba(byte[] buffer, int offset, int length) {
        return new ByteString(wrap(ByteBuffer.wrap(buffer, offset, length)));
    }

    public static ByteString ba(byte[] buffer) {
        return ba(buffer, 0, buffer.length);
    }

    public long length() {
        return buffer.limit() - buffer.position();
    }

    public ByteString copyOf() {
        ByteBuf buf = ByteBuf.allocate(length());
        long pos = buffer.position();
        buf.put(buffer);
        buffer.position(pos);
        buf.flip();
        return new ByteString(buf);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ByteString that = (ByteString) o;

        long len = length();
        if (len != that.length()) {
            return false;
        }

        for (int i = 0; i < len; i++) {
            if (byteAt(i) != that.byteAt(i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        long result = 1;
        long len = length();
        result = 31 * result + len;
        for (int i = 0; i < len; i++) {
            result = 31 * result + byteAt(i);
        }
        return (int) result;
    }


    @Override
    public String toString() {
        long len = Math.min(Integer.MAX_VALUE, length());
        byte buf[] = new byte[(int) len];
        long pos = buffer.position();
        buffer.get(buf, 0, buf.length);
        buffer.position(pos);
        return new String(buf, 0, buf.length);
    }

    public void writeTo(OutputStream out) {
        try {
            long length = length();
            if (length < SMALL_WRITE_TO_THRESHOLD) {
                smallWriteTo(out, length);
                return;
            }

            long bufLen = length;
            if (bufLen > EXCHANGE_BUF_SIZE) bufLen = EXCHANGE_BUF_SIZE;
            byte[] buf = new byte[(int) bufLen];

            long pos = buffer.position();
            while (length > 0) {
                int size = (int) Math.min(buf.length, length);
                buffer.get(buf, 0, size);
                out.write(buf, 0, size);
                length = length();
            }

            buffer.position(pos);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    private void smallWriteTo(OutputStream out, long length) throws IOException {
        for (long i = 0; i < length; i++) {
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
        while (from < to && isSpaceByte(byteAt(from)))
            from++;
        while (from < to && isSpaceByte(byteAt(to - 1)))
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
        return indexOf(ch, 0, length());
    }

    public long indexOf(ByteString str, long start, long end) {
        if (str.length() == 1) {
            return indexOf(str.byteAt(0), start, end);
        }

        long strLen = str.length();
        long wholeLen = end - strLen;
        for (long i = start; i <= wholeLen; i++) {
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

    public long indexOf(byte ch, long start, long end) {
        for (long i = start; i < end; i++) {
            if (byteAt(i) == ch) {
                return i;
            }
        }
        return -1;
    }

    public boolean contains(ByteString key) {
        return indexOf(key, 0, length()) != -1;
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

    public void iterate(ByteString str, Consumer<ByteString> it) {
        iterateIdx(str, (start, end) -> {
            it.accept(substring(start, end));
            return true;
        });
    }

    public void iterateIdx(ByteString separator, SubstringIterator it) {
        iterateIdx(separator, 0, length(), it);
    }

    public void iterateIdx(ByteString separator, long start, long end, SubstringIterator it) {
        if (separator.length() == 1) {
            byte sep = separator.byteAt(0);
            iterateIdx(sep, start, end, it);
            return;
        }
        long s = start;
        while (s < end) {
            long idx = indexOf(separator, s, end);

            if (idx == -1) {
                if (s < end) {
                    it.substring(s, end);
                }
                break;
            }
            if (s < idx && !it.substring(s, idx)) {
                return;
            }
            s = idx + separator.length();
        }
    }

    private void iterateIdx(byte sep, long start, long end, SubstringIterator it) {
        long s = start;
        for (long i = start; i < end; i++) {
            if (byteAt(i) == sep) {
                if (s < i && !it.substring(s, i)) {
                    return;
                }
                s = i + 1;
            }
        }
        if (s < end) {
            it.substring(s, end);
        }
    }

    public ByteString append(ByteString otherStr) {
        ByteBuf buf = wrap(ByteBuffer.allocate((int) (length() + otherStr.length())));
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
        return toLong(10);
    }

    private long toLong(int degree) {
        long r = 0;
        int ptr = 0;

        int sign = 1;
        while (ptr < length() && (byteAt(ptr) == '-' || byteAt(ptr) == '+')) {
            if (byteAt(ptr) == '-') sign *= -1;
            ptr++;
        }

        while (ptr < length()) {
            int d = digitByte(byteAt(ptr), degree);
            if (d == -1) {
                break;
            }
            r *= degree;
            r += d;
            ptr++;
        }
        return r * sign;
    }

    private int digitByte(byte b, int degree) {
        if (degree <= 10) {
            if ('0' <= b && b <= '0' + degree - 1) {
                return b - '0';
            }
        } else {
            if ('0' <= b && b <= '9') {
                return b - '0';
            }
            int off = degree - 11;
            if ('a' <= b && b <= 'a' + off) {
                return b - 'a' + 10;
            } else if ('A' <= b && b <= 'A' + off) {
                return b - 'A' + 10;
            }
        }
        return -1;
    }


    public int toInt() {
        return (int) toLong();
    }

    public int toInt(int degree) {
        return (int) toLong(degree);
    }


    public ByteString fields(ByteString separator, int fieldStart, int fieldEnd) {
        long []r = new long[] { 0,  0, length() };
        fields0(separator, 0, length(), fieldStart, fieldEnd, r);
        return substring(r[1], r[2]);
    }


    public long fieldsIdx(ByteString separator,
                          long start,
                          long end,
                          int fieldStart,
                          int fieldEnd) {
        long []r = new long[] { 0,  0, end };
        if (fieldStart > fieldEnd) throw new IllegalArgumentException("fieldStart");
        iterateIdx(separator, start, end, (s, e) -> {
            if (r[0] == fieldStart) r[1] = s;
            if (r[0] == fieldEnd) r[2] = e;
            r[0]++;

             return r[0] <= fieldStart ||
                     (r[0] <= fieldEnd &&
                             fieldEnd != Integer.MAX_VALUE);
        });
        return encodeIdx(r[1], r[2]);
    }


    private void fields0(ByteString separator,
                         long start,
                         long end,
                         int fieldStart,
                         int fieldEnd,
                         long[] r) {
        if (fieldStart > fieldEnd) throw new IllegalArgumentException("fieldStart");
        iterateIdx(separator, start, end, (s, e) -> {
            if (r[0] == fieldStart) r[1] = s;
            if (r[0] == fieldEnd) r[2] = e;
            r[0]++;

            return r[0] <= fieldStart ||
                    (r[0] <= fieldEnd &&
                            fieldEnd != Integer.MAX_VALUE);
        });
    }

    public ByteString fields(int fieldStart, int fieldEnd) {
        return fields(SEPARATOR, fieldStart, fieldEnd);
    }

    public ByteString field(ByteString separator, int nField) {
        return fields(separator, nField, nField);
    }

    public ByteString field(int nField) {
        return fields(SEPARATOR, nField, nField);
    }

    public ByteString firstField() {
        return field(0);
    }

    public ByteString secondField() {
        return field(1);
    }

    public ByteString thirdField() {
        return field(2);
    }

    public ByteString firstTwoFields() {
        return fields(0, 1);
    }

    public ByteString firstThreeFields() {
        return fields(0, 2);
    }

    public int howMuch(ByteString str) {
        int []n = new int[1];
        iterateIdx(str, (s, e) -> {
            n[0]++;
            return true;
        });
        return n[0];
    }

    public static long idxStart(long encoded) {
        return encoded >> 24;
    }

    public static long idxLen(long encoded) {
        return encoded & ((1 << 24) - 1);
    }

    public static long idxEnd(long encoded) {
        return idxStart(encoded) + idxLen(encoded);
    }

    public static long encodeIdx(long start, long end) {
        long len = end - start;
        return (start << 24) | (len & ((1 << 24) - 1));
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

    public void writeToByIdx(long idx, OutputStream out) {
        long start = idxStart(idx);
        long len = idxLen(idx);
        try {
            for (long i = 0; i < len; i++) {
                out.write((int) byteAt(start + i));
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public ByteString substringIdx(long idx) {
        long start = idxStart(idx);
        long len = idxLen(idx);
        return substring(start, start + len);
    }

    public ByteString replace(byte from, byte to) {
        ByteBuf buf = wrap(ByteBuffer.allocate((int) length()));
        long pos = buffer.position();
        long end = buffer.limit();
        buf.put(buffer);

        for (long i = 0; i < end - pos; i++) {
            if (buf.get(i) == from) {
                buf.put(i, to);
            }
        }

        buffer.position(pos).limit(end);
        buf.position(0).limit(end - pos);
        return new ByteString(buf);
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
}