package byte_lib.string;

import byte_lib.io.ByteFiles;
import byte_lib.buf.ByteBuf;
import byte_lib.buf.BigByteBuf;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import static byte_lib.string.ByteString.*;
import static org.assertj.core.api.Assertions.assertThat;

public class ByteStringTest {
    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testBs() throws Exception {
        ByteString val = bs("abc");
        assertThat(val.length()).isEqualTo(3);
        assertThat(val.byteAt(0)).isEqualTo((byte) 'a');
        assertThat(val.byteAt(1)).isEqualTo((byte) 'b');
        assertThat(val.byteAt(2)).isEqualTo((byte) 'c');
    }

    @Test
    public void testBsCharset() throws Exception {
        ByteString val = bs("abc", Charset.forName("UTF16"));
        assertThat(val.length()).isEqualTo(8);
        assertThat(val.byteAt(0)).isEqualTo((byte) -2);
        assertThat(val.byteAt(1)).isEqualTo((byte) -1);
        assertThat(val.byteAt(2)).isEqualTo((byte) 0);
        assertThat(val.byteAt(3)).isEqualTo((byte) 'a');
        assertThat(val.byteAt(4)).isEqualTo((byte) 0);
        assertThat(val.byteAt(5)).isEqualTo((byte) 'b');
        assertThat(val.byteAt(6)).isEqualTo((byte) 0);
        assertThat(val.byteAt(7)).isEqualTo((byte) 'c');
    }

    @Test
    public void testBa() throws Exception {
        byte []arr = new byte[] {'a','b','c','d','e'};

        ByteString val = ba(arr, 0, 5);

        assertThat(val.length()).isEqualTo(5);
        assertThat(val.byteAt(0)).isEqualTo((byte) 'a');
        assertThat(val.byteAt(1)).isEqualTo((byte) 'b');
        assertThat(val.byteAt(2)).isEqualTo((byte) 'c');
        assertThat(val.byteAt(3)).isEqualTo((byte) 'd');
        assertThat(val.byteAt(4)).isEqualTo((byte) 'e');
    }

    @Test
    public void testBaNoCopy() throws Exception {
        byte []arr = new byte[] {'a','b','c','d','e'};

        ByteString val = ba(arr, 0, 5);

        arr[0] = 'A';

        assertThat(val.byteAt(0)).isEqualTo((byte) 'A');
    }

    @Test
    public void testBb() throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(5);
        buf.put("abcde".getBytes()).flip();

        ByteString val = bb(buf);

        assertThat(val.byteAt(0)).isEqualTo((byte) 'a');
    }

    @Test
    public void testBbNoCopy() throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(5);
        buf.put("abcde".getBytes()).flip();

        ByteString val = bb(buf);

        buf.put(0, (byte) 'A');

        assertThat(val.byteAt(0)).isEqualTo((byte) 'A');
    }

    @Test
    public void testBbDirect() throws Exception {
        ByteBuf buf = new BigByteBuf(5);
        buf.put("abcde".getBytes(), 0, 5).flip();

        ByteString val = bb(buf);

        assertThat(val.byteAt(0)).isEqualTo((byte) 'a');
    }

    @Test
    public void bbDirectNoCopy() throws Exception {
        ByteBuf buf = new BigByteBuf(5);
        buf.put("abcde".getBytes(), 0, 5).flip();

        ByteString val = bb(buf);

        buf.put(0, (byte) 'A');

        assertThat(val.byteAt(0)).isEqualTo((byte) 'A');
    }


    @Test
    public void length() throws Exception {
        byte []arr = new byte[] {'a','b','c','d','e'};

        ByteString val = ba(arr, 1, 3);

        assertThat(val.length())
                .isEqualTo(3);
    }

    @Test
    public void lengthBs() throws Exception {
        ByteString val = bs("abcde");

        assertThat(val.length())
                .isEqualTo(5);
    }

    @Test
    public void copyOf() throws Exception {
        byte []arr = new byte[] {'a','b','c','d','e'};

        ByteString val = ba(arr, 1, 3);

        ByteString val2 = val.copyOf();

        assertThat(val).isEqualTo(bs("bcd"));
        assertThat(val2).isEqualTo(bs("bcd"));
    }

    @Test
    public void copyOfCreatesCopy() throws Exception {
        byte []arr = new byte[] {'a','b','c','d','e'};

        ByteString val = ba(arr, 1, 3);
        ByteString val2 = val.copyOf();

        arr[1] = 'B';

        assertThat(val).isEqualTo(bs("Bcd"));
        assertThat(val2).isEqualTo(bs("bcd"));
    }

    @Test
    public void copyOfGiantMemWorks() throws Exception {
        long moreThanMax = 1L + Integer.MAX_VALUE;
        long moreThanMaxPlusOne = moreThanMax + 1;

        ByteBuf buf = ByteBuf.allocate(moreThanMaxPlusOne);

        buf.put(moreThanMax, (byte) 'a');

        try (ByteString val = bb(buf);
             ByteString val2 = val.copyOf()) {

            buf.put(moreThanMax, (byte) 'A');

            assertThat(val.length()).isEqualTo(moreThanMaxPlusOne);
            assertThat(val.byteAt(moreThanMax)).isEqualTo((byte) 'A');
            assertThat(val2.length()).isEqualTo(moreThanMaxPlusOne);
            assertThat(val2.byteAt(moreThanMax)).isEqualTo((byte) 'a');
        }
    }

    @Test
    public void hasEquals() throws Exception {
        byte []arr = new byte[] {'a','b','c','d','e'};

        ByteString val = ba(arr, 1, 3);

        assertThat(val).isEqualTo(bs("bcd"));
    }

    @Test
    public void notEquals1() throws Exception {
        byte []arr = new byte[] {'a','b','c','d','e'};

        ByteString val = ba(arr, 1, 3);

        assertThat(val).isNotEqualTo(bs("a"));
    }

    @Test
    public void notEquals2() throws Exception {
        byte []arr = new byte[] {'a','b','c','d','e'};

        ByteString val = ba(arr, 1, 3);

        assertThat(val).isNotEqualTo(bs("bca"));
    }

    @Test
    public void hasHashCode() throws Exception {
        byte []arr = new byte[] {'a','b','c','d','e'};

        ByteString val = ba(arr, 1, 3);

        assertThat(val.hashCode()).isNotZero();
    }

    @Test
    public void hasToString() throws Exception {
        byte []arr = new byte[] {'a','b','c','d','c'};

        ByteString val = ba(arr, 1, 3);

        assertThat(val.toString()).isEqualTo("bcd");
    }

    @Test
    public void writeTo() throws Exception {
        byte[] bytes;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            bs("abc\ndef\nghi\n").writeTo(out);
            bytes = out.toByteArray();
        }

        assertThat(new String(bytes))
            .isEqualTo("abc\ndef\nghi\n");
    }

    @Test
    public void writeToAdvanced() throws Exception {
        int size = ByteString.EXCHANGE_BUF_SIZE * 2 + 1234;
        byte[] writtenData;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte []data = new byte[size];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte)i;
            }
            ba(data).writeTo(out);
            writtenData = out.toByteArray();
        }

        for (int i = 0; i < writtenData.length; i++) {
            assertThat(writtenData[i])
                    .describedAs("Byte" + i)
                    .isEqualTo((byte)i);
        }
        assertThat(writtenData)
                .hasSize(size);
    }

    @Test
    public void lastIndexOf() throws Exception {
        byte []arr = new byte[] {'a','c','c','d','c'};

        ByteString val = ba(arr, 1, 3);

        long idx = val.lastIndexOf((byte) 'c');
        assertThat(idx).isEqualTo(1);
    }

    @Test
    public void isEmpty() throws Exception {
        assertThat(EMPTY.isEmpty()).isEqualTo(true);
    }

    @Test
    public void isEmptyNotEmpty() throws Exception {
        byte []arr = new byte[] {'a','c','c','d','c'};

        ByteString val = ba(arr, 1, 3);

        assertThat(val.isEmpty()).isEqualTo(false);
    }

    @Test
    public void trim() throws Exception {
        byte []arr = new byte[] {'a',' ','c',' ','c'};

        ByteString val = ba(arr, 1, 3);

        assertThat(val.trim()).isEqualTo(bs("c"));
    }

    @Test
    public void trimEmpty() throws Exception {
        byte []arr = new byte[] {'a',' ',' ',' ','c'};

        ByteString val = ba(arr, 1, 3);

        assertThat(val.trim()).isEqualTo(bs(""));
    }

    @Test
    public void startsWith() throws Exception {
        byte []arr = new byte[] {'a','c','c','d','c'};

        ByteString val = ba(arr, 1, 3);

        assertThat(val.startsWith(bs("cc"))).isEqualTo(true);
    }

    @Test
    public void endsWith() throws Exception {
        byte []arr = new byte[] {'a','c','c','d','c'};

        ByteString val = ba(arr, 1, 3);

        assertThat(val.endsWith(bs("cd"))).isEqualTo(true);
    }

    @Test
    public void indexOfChar() throws Exception {
        byte []arr = new byte[] {'a','c','c','d','c'};

        ByteString val = ba(arr, 1, 3);

        assertThat(val.indexOf((byte) 'd')).isEqualTo(2);
    }

    @Test
    public void indexOfCharFail() throws Exception {
        byte []arr = new byte[] {'a','c','c','d','c'};

        ByteString val = ba(arr, 1, 3);

        assertThat(val.indexOf((byte) 'z')).isEqualTo(-1);
    }

    @Test
    public void indexOfStringFail() throws Exception {
        byte []arr = new byte[] {'a','c','c','d','c'};

        ByteString val = ba(arr, 1, 3);

        assertThat(val.indexOf(bs("cdd"), 0, 3)).isEqualTo(-1);
    }

    @Test
    public void substring() throws Exception {
        byte []arr = new byte[] {'a','c','c','d','c'};

        ByteString val = ba(arr, 1, 3);

        assertThat(val.substring(2)).isEqualTo(bs("d"));
    }

    @Test
    public void substringTwoArguments() throws Exception {
        byte []arr = new byte[] {'a','c','c','d','c'};

        ByteString val = ba(arr, 1, 3);

        assertThat(val.substring(1, 2)).isEqualTo(bs("c"));
    }

    @Test
    public void split() throws Exception {
        assertThat(bs("a||b||c||d||e").split(bs("||")))
                .containsExactly(
                    bs("a"),
                    bs("b"),
                    bs("c"),
                    bs("d"),
                    bs("e"));
    }

    @Test
    public void append() throws Exception {
        assertThat(bs("a").append(bs("b")))
                .isEqualTo(bs("ab"));
    }

    @Test
    public void compareTo() throws Exception {
        assertThat(bs("a").compareTo(bs("b"))).isEqualTo(-1);
        assertThat(bs("aa").compareTo(bs("bb"))).isEqualTo(-1);
        assertThat(bs("aa").compareTo(bs("b"))).isEqualTo(-1);
        assertThat(bs("aa").compareTo(bs("ab"))).isEqualTo(-1);
        assertThat(bs("a").compareTo(bs("ab"))).isEqualTo(-1);
    }

    @Test
    public void fields() throws Exception {
        assertThat(bs("aa bb cc").firstField()).isEqualTo(bs("aa"));
        assertThat(bs("aa bb cc").secondField()).isEqualTo(bs("bb"));
        assertThat(bs("aa bb cc").thirdField()).isEqualTo(bs("cc"));
        assertThat(bs("aa bb cc").firstTwoFields()).isEqualTo(bs("aa bb"));
        assertThat(bs("aa bb cc dd").firstThreeFields()).isEqualTo(bs("aa bb cc"));
    }

    @Test
    public void load() throws Exception {
        Path tempFile = Files.createTempFile("tmp", ".txt");
        tempFile.toFile().deleteOnExit();
        Files.write(tempFile, "abc\ndef\n".getBytes());
        ByteString str = ByteFiles.readAll(tempFile.toFile().getAbsolutePath());
        assertThat(str).isEqualTo(bs("abc\ndef\n"));
    }

    @Test
    public void cut() throws Exception {
        ByteString val = bs("abcde");

        val = val.cut(bs("ab"), bs("de"));

        assertThat(val.length()).isEqualTo(1);
        assertThat(val.byteAt(0)).isEqualTo((byte) 'c');
    }

}