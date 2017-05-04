package byte_lib.string;

import byte_lib.string.ByteString;
import byte_lib.string.ByteStringBuilder;
import org.junit.Test;

import static byte_lib.string.ByteString.bs;
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