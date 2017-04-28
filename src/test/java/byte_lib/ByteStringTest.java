package byte_lib;

import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static byte_lib.ByteString.bs;

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
    public void wrap() throws Exception {
        byte []arr = new byte[] {'a','b','c','d','e'};

        ByteString val = ByteString.ba(arr, 0, 5);

        assertThat(val.length()).isEqualTo(5);
        assertThat(val.byteAt(0)).isEqualTo((byte) 'a');
        assertThat(val.byteAt(1)).isEqualTo((byte) 'b');
        assertThat(val.byteAt(2)).isEqualTo((byte) 'c');
        assertThat(val.byteAt(3)).isEqualTo((byte) 'd');
        assertThat(val.byteAt(4)).isEqualTo((byte) 'e');
    }

    @Test
    public void cut() throws Exception {
        ByteString val = bs("abcde");

        val = val.cut(bs("ab"), bs("de"));

        assertThat(val.length()).isEqualTo(1);
        assertThat(val.byteAt(0)).isEqualTo((byte) 'c');
    }

    @Test
    public void length() throws Exception {
        byte []arr = new byte[] {'a','b','c','d','e'};

        ByteString val = ByteString.ba(arr, 1, 3);

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

        ByteString val = ByteString.ba(arr, 1, 3);

        ByteString val2 = val.copyOf();

        assertThat(val).isEqualTo(bs("bcd"));
        assertThat(val2).isEqualTo(bs("bcd"));
    }

    @Test
    public void equals() throws Exception {
        byte []arr = new byte[] {'a','b','c','d','e'};

        ByteString val = ByteString.ba(arr, 1, 3);

        assertThat(val).isEqualTo(bs("bcd"));
    }

    @Test
    public void lastIndexOf() throws Exception {
        byte []arr = new byte[] {'a','c','c','d','c'};

        ByteString val = ByteString.ba(arr, 1, 3);

        long idx = val.lastIndexOf((byte) 'c');
        assertThat(idx).isEqualTo(1);
    }

    @Test
    public void substring() throws Exception {
        byte []arr = new byte[] {'a','c','c','d','c'};

        ByteString val = ByteString.ba(arr, 1, 3);

        assertThat(val.substring(2)).isEqualTo(bs("d"));
    }

    @Test
    public void isEmpty() throws Exception {
        byte []arr = new byte[] {'a','c','c','d','c'};

        ByteString val = ByteString.ba(arr, 1, 3);

        assertThat(val.substring(3).isEmpty()).isEqualTo(true);
    }

    @Test
    public void trim() throws Exception {
        byte []arr = new byte[] {'a',' ','c',' ','c'};

        ByteString val = ByteString.ba(arr, 1, 3);

        assertThat(val.trim()).isEqualTo(bs("c"));
    }

    @Test
    public void startsWith() throws Exception {
        byte []arr = new byte[] {'a','c','c','d','c'};

        ByteString val = ByteString.ba(arr, 1, 3);

        assertThat(val.startsWith(bs("cc"))).isEqualTo(true);
    }

    @Test
    public void endsWith() throws Exception {
        byte []arr = new byte[] {'a','c','c','d','c'};

        ByteString val = ByteString.ba(arr, 1, 3);

        assertThat(val.endsWith(bs("cd"))).isEqualTo(true);
    }

    @Test
    public void indexOf() throws Exception {
        byte []arr = new byte[] {'a','c','c','d','c'};

        ByteString val = ByteString.ba(arr, 1, 3);

        assertThat(val.indexOf((byte) 'd')).isEqualTo(2);
    }

    @Test
    public void indexOf1() throws Exception {
        byte []arr = new byte[] {'a','c','c','d','c'};

        ByteString val = ByteString.ba(arr, 1, 3);

        assertThat(val.indexOf(bs("cd"), 0)).isEqualTo(1);
    }

    @Test
    public void substring1() throws Exception {
        byte []arr = new byte[] {'a','c','c','d','c'};

        ByteString val = ByteString.ba(arr, 1, 3);

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
    public void load() throws Exception {
        Path tempFile = Files.createTempFile("tmp", ".txt");
        tempFile.toFile().deleteOnExit();
        Files.write(tempFile, "abc\ndef\n".getBytes());
        ByteString str = ByteString.load(tempFile.toFile().getAbsolutePath());
        assertThat(str).isEqualTo(bs("abc\ndef\n"));
    }
}