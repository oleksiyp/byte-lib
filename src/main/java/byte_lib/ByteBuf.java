package byte_lib;

import java.nio.ByteBuffer;

public interface ByteBuf {
    static ByteBuf allocate(long length) {
        return new DirectByteBuf(length);
    }

    static ByteBuf wrap(ByteBuffer buffer) {
        return new WrappedByteBuf(buffer);
    }

    long limit();

    long position();

    ByteBuf position(long pos);

    ByteBuf limit(long end);

    ByteBuf put(ByteBuf buffer);

    ByteBuf put(byte[] buf, int off, int len);

    ByteBuf get(byte[] buf);

    byte get(long idx);

    ByteBuf put(long index, byte b);

    ByteBuf duplicate();

    ByteBuf flip();

}
