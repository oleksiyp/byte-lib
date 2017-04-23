package byte_lib;

import byte_lib.ByteStringLineAdapter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import rx.Observable;

import java.io.ByteArrayInputStream;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ByteStringLineAdapterTest {
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

    private Queue<String> parse(String line) throws InterruptedException {
        ByteStringLineAdapter adapter;
        adapter = new ByteStringLineAdapter(() ->
                new ByteArrayInputStream(line.getBytes()));

        Queue<String> queue = new ArrayDeque<>();
        CountDownLatch latch = new CountDownLatch(1);
        Observable.create(adapter)
                .doOnTerminate(latch::countDown)
                .forEach((bs) -> queue.add(bs.toString()));

        latch.await();
        return queue;
    }

    private String g(int n) {
        StringBuilder builder = new StringBuilder(n);
        while (n-- > 0) builder.append('g');
        return builder.toString();
    }
}