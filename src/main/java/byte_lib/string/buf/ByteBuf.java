package byte_lib.string.buf;

import java.nio.ByteBuffer;

public interface ByteBuf {
    static ByteBuf allocate(long length) {
        return length <= Integer.MAX_VALUE
                ? wrap(ByteBuffer.allocate((int) length))
                : new BigByteBuf(length);
    }

    static ByteBuf wrap(ByteBuffer buffer) {
        return new WrappedByteBuf(buffer);
    }

    long limit();

    long position();

    ByteBuf position(long pos);

    ByteBuf limit(long end);

    ByteBuf put(byte[] buf, int off, int len);

    ByteBuf put(ByteBuf buffer);

    ByteBuf put(long idx, byte b);

    ByteBuf get(byte[] buf, int off, int len);

    byte get(long idx);

    ByteBuf duplicate();

    ByteBuf flip();

}
