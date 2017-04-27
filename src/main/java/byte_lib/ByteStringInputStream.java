package byte_lib;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteBuffer.allocateDirect;

public class ByteStringInputStream extends InputStream {
    private final InputStream in;
    private byte []buf;
    private boolean eof;
    private boolean done;
    private int ptr;
    private int bufSz;

    public ByteStringInputStream(InputStream in) {
        this.in = in;
        buf = new byte[1024*64];
    }

    public int countBytes() throws IOException {
        long sz = 0;
        while (!eof) {
            readMore();
            sz += bufSz;
            bufSz = 0;
        }
        return (int) sz;
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

    public  ByteString readLine() throws IOException {
        while (!done) {
            int idx = searchNewLine(buf, ptr, bufSz);
            if (idx != -1) {
                int oldPtr = ptr;
                ptr = skipDoubleNewLine(buf, idx, bufSz) + 1;
                return ByteString.wrap(buf, oldPtr, idx - oldPtr);
            }

            if (eof) {
                done = true;
                if (ptr < bufSz) {
                    return ByteString.wrap(buf, ptr, bufSz - ptr);
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

    public ByteBuffer readAll(boolean direct, int sz, Progress progress) throws IOException {
        progress = Progress.voided(progress);

        ByteBuffer result = direct ? allocateDirect(sz) : allocate(sz);
        while (!eof) {
            readMore();
            result.put(buf, 0, bufSz);
            sz += bufSz;
            bufSz = 0;
            progress.progress(bufSz);
        }
        result.flip();
        return result;
    }

    public static ByteStringInputStream file(String path) throws IOException {
        return file(new File(path));
    }

    public static ByteStringInputStream file(File file) throws IOException {
        InputStream in = new FileInputStream(file);
        if (file.getName().endsWith(".gz")) {
            in = new GZIPInputStream(in);
        } else if (file.getName().endsWith(".bz2")) {
            in = new BZip2CompressorInputStream(in);
        }
        return new ByteStringInputStream(in);
    }

    public void readLines(Consumer<ByteString> lines) {
        ByteString line;
        while ((line = nextLine()) != null) {
            lines.accept(line);
        }
    }

    public static ByteStringInputStream string(String str) {
        return new ByteStringInputStream(new ByteArrayInputStream(str.getBytes()));
    }
}
