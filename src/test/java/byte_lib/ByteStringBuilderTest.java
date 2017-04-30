package byte_lib;

import org.junit.Test;

import static byte_lib.ByteString.bs;
import static org.assertj.core.api.Assertions.assertThat;

public class ByteStringBuilderTest {
    @Test
    public void append() throws Exception {
        ByteString str = new ByteStringBuilder()
                .append(bs("abc"))
                .append((byte) ' ')
                .append(bs("def"))
                .build();

        assertThat(str).isEqualTo(bs("abc def"));
    }
}