package byte_lib;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static byte_lib.ByteString.bs;
import static org.assertj.core.api.Assertions.assertThat;

public class SimpleByteStringHashTest {
    @Test
    public void hash() throws Exception {
        ByteStringHash hash = new ByteStringHash.SimpleByteStringHash();
        Set<Long> hashes = new HashSet<>();
        for (int i = 0; i < 65536; i++) {
            long l = hash.n(i).getHash(bs("test"));
            hashes.add(l);
        }
        assertThat(hashes).hasSize(65536);
    }
}