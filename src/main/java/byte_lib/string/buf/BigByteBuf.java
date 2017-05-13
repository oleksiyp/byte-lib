package byte_lib.string.buf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.*;

import java.lang.reflect.Field;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;

import static java.lang.String.format;

public class BigByteBuf implements ByteBuf {
    private static final Logger LOG = LoggerFactory.getLogger(BigByteBuf.class);

    public static final long COPY_SIZE = 1024L * 1024;
    public static final int SMALL_BUFFER_SIZE = 6;
    static Unsafe unsafe = getUnsafe();

    private static final long arrayBaseOffset = (long)unsafe.arrayBaseOffset(byte[].class);

    private final Cleaner cleaner;
    private final long size;
    private final long base;
    private long position;
    private long limit;
    private final Object attached;

    public BigByteBuf(long size){
        base = allocate(size);
        LOG.info("Allocated {} big buffer", size);
        this.size = size;
        this.limit = size;
        cleaner = Cleaner.create(this, new Deallocator(base, size));
        attached = null;
    }

    private long allocate(long size) {
        doCleaning();
        return unsafe.allocateMemory(size);
    }

    BigByteBuf(BigByteBuf buf) {
        attached = buf;
        cleaner = null;
        base = buf.base;
        position = buf.position;
        limit = buf.limit;
        size = buf.size;
    }

    @Override
    public long limit() {
        return limit;
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public ByteBuf position(long p) {
        position = p;
        return this;
    }

    @Override
    public ByteBuf limit(long l) {
        limit = l;
        return null;
    }

    @Override
    public ByteBuf put(ByteBuf src) {
        if (src instanceof BigByteBuf) {
            if (src == this) {
                throw new IllegalArgumentException();
            }
            BigByteBuf sb = (BigByteBuf)src;

            long spos = sb.position();
            long slim = sb.limit();

            long srem = (spos <= slim ? slim - spos : 0);

            long pos = position();
            long lim = limit();
            assert (pos <= lim);
            long rem = (pos <= lim ? lim - pos : 0);

            if (srem > rem)
                throw new BufferOverflowException();
            unsafe.copyMemory(sb.base + spos, base + pos, srem);
            sb.position(spos + srem);
            position(pos + srem);
        } else {
            long j = src.position();
            long n = src.limit() - src.position();
            for (long i = 0; i < n; i++) {
                put(src.get(j++));
            }
            src.position(j);
        }
        return this;
    }

    @Override
    public ByteBuf put(byte[] src, int offset, int length) {
        if (length <= SMALL_BUFFER_SIZE) {
            putSmallBuffer(src, offset, length);
            return this;
        }
        checkBounds(offset, length, src.length);
        long pos = position();
        long lim = limit();
        long rem = (pos <= lim ? lim - pos : 0);
        if (length > rem)
            throw new BufferOverflowException();

        copyFromArray(src, arrayBaseOffset, offset, base + pos, length);
        position(pos + length);
        return this;
    }

    private void putSmallBuffer(byte[] src, int offset, int length) {
        int j = offset;
        for (long i = 0; i < length; i++) {
            put(src[j++]);
        }
    }

    public ByteBuf get(byte[] dst, int offset, int length) {
        if (length <= SMALL_BUFFER_SIZE) {
            getSmallBuffer(dst, offset, length);
            return this;
        }
        checkBounds(offset, length, dst.length);
        long pos = position();
        long lim = limit();
        assert (pos <= lim);
        long rem = (pos <= lim ? lim - pos : 0);
        if (length > rem) {
            throw new BufferUnderflowException();
        }

        copyToArray(base + pos, dst, arrayBaseOffset, offset, length);
        position(pos + length);
        return this;
    }

    private void getSmallBuffer(byte[] dst, int offset, int length) {
        checkBounds(offset, length, dst.length);
        if (length > limit() - position()) {
            throw new BufferUnderflowException();
        }
        int end = offset + length;
        for (int i = offset; i < end; i++) {
            dst[i] = get();
        }
    }

    private static void checkBounds(int off, int len, int size) { // package-private
        if ((off | len | (off + len) | (size - (off + len))) < 0)
            throw new IndexOutOfBoundsException();
    }

    private static void copyFromArray(Object src,
                                      long srcBaseOffset,
                                      long srcPos,
                                      long dstAddr,
                                      long length)
    {
        long offset = srcBaseOffset + srcPos;
        while (length > 0) {
            long size = (length > COPY_SIZE) ? COPY_SIZE : length;
            unsafe.copyMemory(src, offset, null, dstAddr, size);
            length -= size;
            offset += size;
            dstAddr += size;
        }
    }

    private static void copyToArray(long srcAddr,
                                    Object dst,
                                    long dstBaseOffset,
                                    long dstPos,
                                    long length)
    {
        long offset = dstBaseOffset + dstPos;
        while (length > 0) {
            long size = (length > COPY_SIZE) ? COPY_SIZE : length;
            unsafe.copyMemory(null, srcAddr, dst, offset, size);
            length -= size;
            srcAddr += size;
            offset += size;
        }
    }


    public byte get() {
        return unsafe.getByte(base + nextGetIndex());
    }


    public byte get(long idx) {
        return unsafe.getByte(base + checkIndex(idx));
    }

    @Override
    public ByteBuf put(long idx, byte b) {
        unsafe.putByte(base + checkIndex(idx), b);
        return this;
    }

    public ByteBuf put(byte b) {
        unsafe.putByte(base + nextPutIndex(), b);
        return this;
    }

    @Override
    public ByteBuf duplicate() {
        return new BigByteBuf(this);
    }

    @Override
    public ByteBuf flip() {
        limit = position;
        position = 0;
        return this;
    }

    @Override
    public void free() {
        cleaner.clean();
    }

    private long checkIndex(long idx) {
        if (idx < 0 || idx >= limit) {
            throw new IndexOutOfBoundsException();
        }
        return idx;
    }

    private long nextGetIndex() {
        if (position >= limit) {
            throw new BufferUnderflowException();
        }
        return position++;
    }

    private long nextPutIndex() {
        if (position >= limit) {
            throw new BufferOverflowException();
        }
        return position++;
    }

    private class Deallocator implements Runnable {
        private final long base;
        private final long size;

        public Deallocator(long base, long size) {
            this.base = base;
            this.size = size;
        }

        @Override
        public void run() {
            unsafe.freeMemory(base);
            LOG.info("Deallocated {}", size);
        }
    }

    private static Unsafe getUnsafe() {
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            return (Unsafe) unsafeField.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void doCleaning() {
        SharedSecrets.getJavaLangRefAccess().tryHandlePendingReference();
        System.gc();
    }

}
