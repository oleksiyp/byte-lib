package byte_lib.io;

import byte_lib.io.ByteStringInputStream;
import byte_lib.string.ByteString;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

public class ByteStringInputStreamTest {
    @Test
    public void testReadingLines1() throws Exception {
        Queue<String> queue = parse("line1\nline2\nline3\n");

        assertThat(queue).containsExactly("line1", "line2", "line3");
    }

    @Test
    public void testReadingLines2() throws Exception {
        Queue<String> queue = parse("line1\nline2\nline3");

        assertThat(queue).containsExactly("line1", "line2", "line3");
    }

    @Test
    public void testReadingLines3() throws Exception {
        Queue<String> queue = parse("line1\nli" + g(4096) + "ne2\nline3");

        assertThat(queue).containsExactly("line1", "li" + g(4096) + "ne2", "line3");
    }

    @Test
    public void testReadingLines4() throws Exception {
        Queue<String> queue = parse("lin" + g(4096) + "e1\nline2\nline3");

        assertThat(queue).containsExactly("lin" + g(4096) + "e1", "line2", "line3");
    }

    @Test
    public void testReadingLines5() throws Exception {
        Queue<String> queue = parse("line1\n\nline2\nline3");

        assertThat(queue).containsExactly("line1", "", "line2", "line3");
    }

    @Test
    public void testReadingLines6() throws Exception {
        Queue<String> queue = parse("line1\n\rline2\nline3");

        assertThat(queue).containsExactly("line1", "line2", "line3");
    }

    private Queue<String> parse(String str) {
        try (ByteStringInputStream in = new ByteStringInputStream(new ByteArrayInputStream(str.getBytes()))) {
            Queue<String> result = new ArrayDeque<>();
            ByteString s;
            while ((s = in.readLine()) != null) {
                result.add(s.toString());
            }
            return result;
        } catch (IOException e) {
            return null;
        }
    }


    private String g(int n) {
        StringBuilder builder = new StringBuilder(n);
        while (n-- > 0) builder.append('g');
        return builder.toString();
    }

}