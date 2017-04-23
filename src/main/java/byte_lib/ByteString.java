package byte_lib;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class ByteString implements Comparable<ByteString> {
    public static final ByteString EMPTY = new ByteString(ByteBuffer.allocate(0));

    private final ByteBuffer buffer;
    private final int pos;
    private final int end;

    public ByteString(ByteBuffer buffer) {
        this.buffer = buffer;
        this.pos = buffer.position();
        this.end = buffer.limit();
    }

    public static ByteString bs(String s) {
        byte[] buf = s.getBytes();
        ByteBuffer buffer = ByteBuffer.allocateDirect(buf.length);
        buffer.put(buf).flip();
        return new ByteString(buffer);
    }

    public static ByteString bs(String s, Charset charset) {
        byte[] buf = s.getBytes(charset);
        ByteBuffer buffer = ByteBuffer.allocateDirect(buf.length);
        buffer.put(buf).flip();
        return new ByteString(buffer);
    }

    public static ByteString wrap(byte[] buffer, int offset, int length) {
        return new ByteString(ByteBuffer.wrap(buffer, offset, length));
    }

    public ByteString cut(ByteString start, ByteString end) {
        int sIdx = 0;
        if (startsWith(start)) {
            sIdx += start.length();
        } else {
            return null;
        }

        int eIdx = length();
        if (endsWith(end)) {
            eIdx -= end.length();
        } else {
            return null;
        }

        return substring(sIdx, eIdx);
    }

    public int length() {
        return buffer.limit() - buffer.position();
    }

    public ByteString copyOf() {
        return copyOf(true);
    }

    public ByteString copyOf(boolean direct) {
        ByteBuffer buf = direct ? ByteBuffer.allocateDirect(length()) : ByteBuffer.allocate(length());
        buf.put(buffer);
        resetPos();
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
        int result = 1;
        result = 31 * result + length();
        for (int i = 0; i < length(); i++) {
            result = 31 * result + byteAt(i);
        }
        return result;
    }

    public long longHash() {
        return longHash(1L);
    }

    public long longHash(long hash) {
        hash = 31 * hash + length();
        for (int i = 0; i < length(); i++) {
            hash = 31 * hash + byteAt(i);
        }
        return hash;
    }


    @Override
    public String toString() {
        byte buf[] = new byte[length()];
        buffer.get(buf);
        resetPos();
        return new String(buf, 0, buf.length);
    }

    private void resetPos() {
        buffer.position(pos).limit(end);
    }

    public void writeTo(PrintStream out) {
        out.write(buffer.array(), buffer.position(), length());
    }

    public byte byteAt(int index) {
        return buffer.get(index + buffer.position());
    }

    public int lastIndexOf(byte ch) {
        for (int i = length() - 1; i >= 0; i--) {
            if (byteAt(i) == ch) {
                return i;
            }
        }
        return -1;
    }

    public ByteString substring(int from, int to) {
        ByteBuffer buf = buffer.duplicate();
        buf.position(pos + from).limit(pos + to);
        return new ByteString(buf);
    }

    public boolean isEmpty() {
        return length() == 0;
    }

    public ByteString trim() {
        int from = 0;
        int to = length();
        while (isSpaceByte(byteAt(from)) && from < to)
            from++;
        while (isSpaceByte(byteAt(to - 1)) && from < to)
            to--;
        return substring(from, to);
    }

    private boolean isSpaceByte(byte b) {
        return b == ' ' || b == '\n' || b == '\t' || b == '\r';
    }

    public boolean startsWith(ByteString string) {
        if (string.length() > length()) {
            return false;
        }
        for (int i = 0; i < string.length(); i++) {
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
        int off = length() - string.length();
        for (int i = 0; i < string.length(); i++) {
            if (string.byteAt(i) != byteAt(off + i)) {
                return false;
            }
        }
        return true;
    }

    public int indexOf(byte ch) {
        for (int i = 0; i < length(); i++) {
            if (byteAt(i) == ch) {
                return i;
            }
        }
        return -1;
    }

    public int indexOf(ByteString str, int off) {
        for (int i = off; i <= length() - str.length(); i++) {
            boolean found = true;
            for (int j = 0; j < str.length(); j++) {
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

    public ByteString substring(int start) {
        return substring(start, length());
    }

    public ByteString[] split(ByteString str) {
        int start = 0;
        List<ByteString> arr = new ArrayList<>();
        while (start < length()) {
            int idx = indexOf(str, start);
            if (idx == -1) {
                if (start < length()) {
                    arr.add(substring(start, length()));
                }
                break;
            }
            arr.add(substring(start, idx));
            start = idx + str.length();
        }
        return arr.toArray(new ByteString[arr.size()]);
    }

    public ByteString append(ByteString otherStr) {
        ByteBuffer buf = ByteBuffer.allocateDirect(length() + otherStr.length());
        buf.put(buffer)
                .put(otherStr.buffer);
        resetPos();
        otherStr.resetPos();
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

}