package byte_lib.io;

import byte_lib.Progress;
import byte_lib.string.buf.ByteBuf;
import byte_lib.string.ByteString;

import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.function.Consumer;

public class ByteStringInputStream extends InputStream {
    private final InputStream in;
    private byte []buf;
    private boolean eof;
    private boolean done;
    private int ptr;
    private int bufSz;

    public ByteStringInputStream(InputStream in) {
        this.in = in;
        buf = new byte[1024*512];
    }

    public long countBytes() throws IOException {
        long sz = 0;
        while (!eof) {
            readMore();
            sz += bufSz;
            bufSz = 0;
        }
        return sz;
    }

    public ByteString readLine() throws IOException {
        while (!done) {
            int idx = searchNewLine(buf, ptr, bufSz);
            if (idx != -1) {
                int oldPtr = ptr;
                ptr = skipDoubleNewLine(buf, idx, bufSz) + 1;
                return ByteString.ba(buf, oldPtr, idx - oldPtr);
            }

            if (eof) {
                done = true;
                if (ptr < bufSz) {
                    return ByteString.ba(buf, ptr, bufSz - ptr);
                }
                return null;
            }

            compactBuf();
            doubleBufIfFull();
            readMore();
        }

        return null;
    }

    private void readMore() throws IOException {
        int read = in.read(buf, bufSz, buf.length - bufSz);
        if (read == 0) {
            return;
        }
        if (read == -1) {
            read = 0;
            eof = true;
        }

        bufSz += read;
    }

    private void doubleBufIfFull() {
        if (buf.length == bufSz) {
            buf = Arrays.copyOf(buf, buf.length * 2);
        }
    }

    private void compactBuf() {
        System.arraycopy(buf, ptr, buf, 0, bufSz - ptr);
        bufSz -= ptr;
        ptr = 0;
    }

    private int searchNewLine(byte[] buf, int from, int to) {
        for (int i = from; i < to; i++) {
            if (buf[i] == '\n' || buf[i] == '\r') {
                return i;
            }
        }
        return -1;
    }

    private int skipDoubleNewLine(byte[] buf, int from, int to) {
        if (from + 1 < to) {
            if (buf[from] == '\n' && buf[from + 1] == '\r') {
                return from + 1;
            }
            if (buf[from] == '\r' && buf[from + 1] == '\n') {
                return from + 1;
            }
        }
        return from;
    }


    @Override
    public int read() throws IOException {
        while (!done) {
            if (ptr < bufSz) {
                return buf[ptr++];
            }

            if (eof) {
                done = true;
                if (ptr < bufSz) {
                    return buf[ptr++];
                }
                return -1;
            }

            compactBuf();
            doubleBufIfFull();
            readMore();
        }

        return -1;
    }

    public ByteString nextLine() {
        try {
            return readLine();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }


    public int countLines() {
        try (ByteStringInputStream in = this) {
            int cnt = 0;
            while (in.nextLine() != null) {
                cnt++;
            }
            return cnt;
        } catch (IOException ex) {
            throw new IOError(ex);
        }
    }

    public ByteBuf readAll(long sz, Progress progress) throws IOException {
        progress = Progress.voidIfNull(progress);

        ByteBuf result = ByteBuf.allocate(sz);
        progress.reset(sz);
        while (!eof) {
            readMore();
            progress.progress(bufSz);
            result.put(buf, 0, bufSz);
            bufSz = 0;
        }
        result.flip();
        return result;
    }

    public void readLines(Consumer<ByteString> lines) {
        ByteString line;
        while ((line = nextLine()) != null) {
            lines.accept(line);
        }
    }

}
