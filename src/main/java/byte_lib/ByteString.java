package byte_lib;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static byte_lib.ByteStringInputStream.file;
import static java.lang.String.format;

public class ByteString implements Comparable<ByteString> {
    public static final ByteString EMPTY = new ByteString(ByteBuffer.allocate(0));
    public static final ByteString SEPARATOR = bs(" ");
    public static final ByteString NEW_LINE = bs("\n");

    private final ByteBuffer buffer;

    public ByteString(ByteBuffer buffer) {
        this.buffer = buffer;
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
        int pos = buffer.position();
        int end = buffer.limit();
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
        int pos = buffer.position();
        int end = buffer.limit();
        buffer.get(buf);
        buffer.position(pos).limit(end);
        return new String(buf, 0, buf.length);
    }

    public void writeTo(PrintStream out) {
        for (int i = 0; i < length(); i++) {
            out.write(byteAt(i));
        }
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
        int pos = buffer.position();
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
        return indexOf(ch, 0);
    }

    public int indexOf(ByteString str, int off) {
        if (str.length() == 1) {
            return indexOf(str.byteAt(0), off);
        }

        int strLen = str.length();
        int wholeLen = length() - strLen;
        for (int i = off; i <= wholeLen; i++) {
            boolean found = true;
            for (int j = 0; j < strLen; j++) {
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

    public int indexOf(byte ch, int off) {
        int wholeLen = length();
        for (int i = off; i < wholeLen; i++) {
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

    public void iterate(ByteString str, Consumer<ByteString> it) {
        iterateIdx(str, (start, end) -> it.accept(substring(start, end)));
    }

    public void iterateIdx(ByteString str, BiConsumer<Integer, Integer> it) {
        int start = 0;
        while (start < length()) {
            int idx = indexOf(str, start);

            if (idx == -1) {
                if (start < length()) {
                    it.accept(start, length());
                }
                break;
            }
            it.accept(start, idx);
            start = idx + str.length();
        }
    }

    public ByteString append(ByteString otherStr) {
        ByteBuffer buf = ByteBuffer.allocateDirect(length() + otherStr.length());
        int pos = buffer.position();
        int end = buffer.limit();

        int otherPos = otherStr.buffer.position();
        int otherEnd = otherStr.buffer.limit();

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

    public static ByteString bb(ByteBuffer buf) {
        return new ByteString(buf);
    }

    public ByteString fields(ByteString separator, int start, int end) {
        if (start > end) throw new IllegalArgumentException("start");
        int []r = new int[] { 0,  0, length() };
        iterateIdx(separator, (s, e) -> {
            if (r[0] == start) r[1] = s;
            if (r[0] == end) r[2] = e;
            r[0]++;
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
        return load(new File(path), null, true);
    }

    public static ByteString load(String path, Progress progress) throws IOException {
        return load(new File(path), progress, true);
    }

    public static ByteString load(File file, Progress progress, boolean direct) throws IOException {
        String name = file.getName();
        progress = Progress.voided(progress);

        progress.message(format("Counting size of '%s'", name));
        int nBytes;
        try (ByteStringInputStream in = file(file)) {
            nBytes = in.countBytes();
        }

        progress.reset(nBytes);
        progress.message(format("Allocating %s for '%s'", Bytes.sizeToString(nBytes), name));
        try (ByteStringInputStream in = file(file)){
            ByteBuffer buf = in.readAll(direct, nBytes, progress);
            return ByteString.bb(buf);
        } finally {
            progress.message(format("Done reading '%s'!", name));
        }
    }

    public int howMuch(ByteString str) {
        int []n = new int[1];
        iterateIdx(str, (s, e) -> n[0]++);
        return n[0];
    }
}