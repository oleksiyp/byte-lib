package byte_lib;

import org.junit.Test;

import static byte_lib.ByteString.bs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class BloomByteStringFilterTest {
    @Test
    public void test1() throws Exception {
        ByteStringFilter filter = new BloomByteStringFilter(16, 16);

        filter.add(bs("aaa"));
        assertThat(filter.contains(bs("aaa"))).isTrue();
    }

    @Test
    public void test2() throws Exception {
        ByteStringFilter filter = new BloomByteStringFilter(16, 16);

        filter.add(bs("aaa"));
        filter.add(bs("bbb"));
        filter.add(bs("ccc"));
        filter.add(bs("ddd"));
        filter.add(bs("eee"));

        assertThat(filter.contains(bs("aaa"))).isTrue();
        assertThat(filter.contains(bs("bbb"))).isTrue();
        assertThat(filter.contains(bs("ccc"))).isTrue();
        assertThat(filter.contains(bs("ddd"))).isTrue();
        assertThat(filter.contains(bs("eee"))).isTrue();

        assertThat(filter.contains(bs("a"))).isFalse();
        assertThat(filter.contains(bs("b"))).isFalse();
        assertThat(filter.contains(bs("c"))).isFalse();
        assertThat(filter.contains(bs("d"))).isFalse();
        assertThat(filter.contains(bs("ggg"))).isFalse();
    }

}