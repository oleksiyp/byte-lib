package byte_lib.string.buf;

import java.nio.*;

public class WrappedByteBuf implements ByteBuf {
    private ByteBuffer buffer;

    public WrappedByteBuf(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public ByteBuf duplicate() {
        return new WrappedByteBuf(buffer.duplicate());
    }

    @Override
    public ByteBuf flip() {
        buffer.flip();
        return this;
    }

    public byte get(long index) {
        return buffer.get((int) index);
    }

    public ByteBuf put(long idx, byte b) {
        buffer.put((int) idx, b);
        return this;
    }

    @Override
    public ByteBuf get(byte[] buf, int off, int len) {
        buffer.get(buf, off, len);
        return this;
    }


    @Override
    public ByteBuf put(byte[] src, int offset, int length) {
        buffer.put(src, offset, length);
        return this;
    }

    @Override
    public ByteBuf put(ByteBuf buf) {
        if (buf instanceof WrappedByteBuf) {
            buffer.put(((WrappedByteBuf) buf).buffer);
        } else {
            long j = buf.position();
            long n = buf.limit() - buf.position();
            for (long i = 0; i < n; i++) {
                buffer.put(buf.get(j++));
            }
            buf.position(j);
        }
        return this;
    }

    @Override
    public String toString() {
        return buffer.toString();
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }

    @Override
    public boolean equals(Object ob) {
        if (!(ob instanceof WrappedByteBuf)) {
            return false;
        }
        return buffer.equals(((WrappedByteBuf) ob).buffer);
    }

    @Override
    public long limit() {
        return buffer.limit();
    }

    @Override
    public long position() {
        return buffer.position();
    }

    public ByteBuf position(long newPosition) {
        buffer.position((int) newPosition);
        return this;
    }

    public ByteBuf limit(long newLimit) {
        buffer.limit((int) newLimit);
        return this;
    }

    @Override
    public void free() {
        buffer = null;
    }

}
