package byte_lib.string;

import java.nio.ByteBuffer;

public class ByteStringBuilder {
    private ByteBuffer buf;

    public ByteStringBuilder() {
        buf = ByteBuffer.allocate(16);
    }

    public ByteStringBuilder(long len) {
        buf = ByteBuffer.allocate(1 << Util.nBits(len));
    }

    public ByteStringBuilder clear() {
        buf.position(0).limit(buf.capacity());
        return this;
    }

    public ByteString build() {
        buf.flip();
        return ByteString.bb(buf);
    }

    public ByteStringBuilder append(ByteString str) {
        for (int i = 0; i < str.length(); i++) {
            append(str.byteAt(i));
        }
        return this;
    }

    public ByteStringBuilder append(byte b) {
        if (buf.position() == buf.limit()) {
            extendTwice();
        }
        buf.put(b);
        return this;
    }

    private void extendTwice() {
        ByteBuffer newBuf = ByteBuffer.allocate(buf.capacity() * 2);
        buf.flip();
        newBuf.put(buf);
        buf = newBuf;
    }
}
