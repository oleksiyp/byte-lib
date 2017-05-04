package byte_lib.hashed;

import byte_lib.hashed.ByteStringMap;
import byte_lib.string.ByteString;
import org.junit.Test;

import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static byte_lib.string.ByteString.bs;

public class ByteStringMapTest {
    @Test
    public void removeIsEmpty() throws Exception {
        ByteStringMap<Integer> map = new ByteStringMap<>(4);
        map.put(bs("abc"), 5);
        assertThat(map.isEmpty()).isEqualTo(false);
        map.remove(bs("abc"));
        assertThat(map.isEmpty()).isEqualTo(true);
        map.put(bs("abc"), 6);
        assertThat(map.isEmpty()).isEqualTo(false);
        map.remove(bs("abc"));
        assertThat(map.isEmpty()).isEqualTo(true);
        map.put(bs("abc"), 7);
        assertThat(map.isEmpty()).isEqualTo(false);
        map.remove(bs("abc"));
        assertThat(map.isEmpty()).isEqualTo(true);
    }

    @Test
    public void walkingPutRemove() throws Exception {
        Random rnd = new Random(5);
        ByteStringMap<Object> map = new ByteStringMap<>();
        for (int j = 0; j < 10; j++) {

            int base = rnd.nextInt(1000);
            int len = rnd.nextInt(1000);

            for (int i = base; i < base + len; i++) {
                map.put(bs("" + i), i);
            }

            base = rnd.nextInt(1000);
            len = rnd.nextInt(1000);

            for (int i = base; i < base + len; i++) {
                map.remove(bs("" + i));
            }
        }
        for (int i = 0; i < 2000; i++) {
            map.remove(bs("" + i));
        }
        assertThat(map).isEmpty();
        assertThat(map.entrySet()).isEmpty();
    }


    @Test
    public void sameHashCode() throws Exception {
        String sameHashMod256[][] = {
            {"6e", "a0", "123", "1bc", "244", "2dd", "365", "3fe", "486", "640", "761", "7fa", "882", "ab3", "b1a" },
            {"4c", "101", "189", "222", "2bb", "343", "3dc", "464", "4fd", "585", "860", "981", "a5f", "bb2", "cd3"}
        };

        Map<ByteString, Integer> hk = new ByteStringMap<>(150);
        for (int i = 0; i < sameHashMod256[0].length; i++) {
            for (int j = 0; j < 2; j++) {
                hk.put(bs(sameHashMod256[j][i]),
                        sameHashMod256[j][i].hashCode());
            }
        }

        for (int i = 0; i < sameHashMod256[0].length; i++) {
            hk.remove(bs(sameHashMod256[0][i]));
        }

        for (int i = 0; i < sameHashMod256[0].length; i++) {
            assertThat(hk.get(bs(sameHashMod256[0][i])))
                    .describedAs(sameHashMod256[0][i])
                    .isEqualTo(null);

            assertThat(hk.get(bs(sameHashMod256[1][i])))
                    .describedAs(sameHashMod256[1][i])
                    .isEqualTo(sameHashMod256[1][i].hashCode());
        }
    }
}