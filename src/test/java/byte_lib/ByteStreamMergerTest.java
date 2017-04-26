package byte_lib;

import byte_lib.ByteStreamMerger;
import byte_lib.ByteString;
import byte_lib.ByteStringInputStream;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static byte_lib.ByteStringInputStream.string;
import static org.assertj.core.api.Assertions.assertThat;

public class ByteStreamMergerTest {
    @Test
    public void testMissingRecord() throws Exception {
        ByteStringInputStream in1 = string("aaa 3\nabc 5\ndef 1\nghi 2\n");
        ByteStringInputStream in2 = string("abc 6\nghi 5\n");

        List<Integer> result = new ArrayList<>();
        new ByteStreamMerger(in1, in2).merge((arr) -> result.add(sumFirstFields(arr[0], arr[1])));

        assertThat(result).containsExactly(11, 7);
    }

    @Test
    public void testAllRecords() throws Exception {
        ByteStringInputStream in1 = string("abc 5\ndef 1\nghi 2\n");
        ByteStringInputStream in2 = string("abc 6\ndef 3\nghi 5\n");

        List<Integer> result = new ArrayList<>();
        new ByteStreamMerger(in1, in2).merge((arr) -> result.add(sumFirstFields(arr[0], arr[1])));

        assertThat(result).containsExactly(11, 4, 7);
    }

    private int sumFirstFields(ByteString a, ByteString b) {
        return a.field(1).toInt() + b.field(1).toInt();
    }
}