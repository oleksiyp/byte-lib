package byte_lib;

import java.nio.ByteBuffer;

public class ByteStringBuilder {
    ByteBuffer buf;

    public ByteStringBuilder() {
        buf = ByteBuffer.allocate(16);
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
            if (buf.position() == buf.limit()) {
                extendTwice();
            }
            buf.put(str.byteAt(i));
        }
        return this;
    }

    private void extendTwice() {
        ByteBuffer newBuf = ByteBuffer.allocate(buf.capacity() * 2);
        buf.flip();
        newBuf.put(buf);
        buf = newBuf;
    }
}
