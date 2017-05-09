package byte_lib.hashed;

import org.junit.Test;

import static byte_lib.string.ByteString.bs;
import static org.assertj.core.api.Assertions.assertThat;

public class IdxByteStringMultiMapTest {
    @Test
    public void testMultiMapGathersKeys() throws Exception {
        IdxByteStringMultiMap map = new IdxByteStringMultiMap(
                bs("abc def;ghi klm;abc ddd;ghi ppp;fff aaa;abc aaa"),
                bs(";"),
                IdxMapper::firstField,
                IdxMapper::secondField);

        assertThat(map.get(bs("abc"))).containsExactly(bs("def"), bs("ddd"), bs("aaa"));
        assertThat(map.get(bs("ghi"))).containsExactly(bs("klm"), bs("ppp"));
        assertThat(map.get(bs("fff"))).containsExactly(bs("aaa"));
        assertThat(map.get(bs("ggg"))).isEmpty();
    }
}