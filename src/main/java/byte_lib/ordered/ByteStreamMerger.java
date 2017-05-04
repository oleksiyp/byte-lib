package byte_lib.ordered;

import byte_lib.string.ByteString;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Comparator.comparing;

public class ByteStreamMerger {
    private final Supplier<ByteString>[] in;
    private final boolean[] takeNext;
    private final ByteString[] lines;
    private Comparator<ByteString> streamCmp;
    private Comparator<ByteString> recordCmp;

    ByteStreamMerger(Supplier ...in) {
        this.in = in;
        lines = new ByteString[in.length];
        takeNext = new boolean[in.length];
        Arrays.fill(takeNext, true);
        streamCmp = Comparator.naturalOrder();
        recordCmp = comparing(ByteString::firstField);
    }

    public ByteString peekLine(int idx) {
        if (takeNext[idx]) {
            lines[idx] = in[idx].get();
            takeNext[idx] = false;
        }
        return lines[idx];
    }

    public void merge(Consumer<ByteString[]> consumer) {

        while (takeMin() != null) {
            if (checkAreSame()) {
                consumer.accept(lines);
            }
        }
    }

    private boolean checkAreSame() {
        if (lines[0] == null) {
            return false;
        }
        for (int i = 0; i < lines.length - 1; i++) {
            if (lines[i + 1] == null)  {
                return false;
            }
            if (recordCmp.compare(lines[i], lines[i+1]) != 0) {
                return false;
            }
        }
        return true;
    }

    private ByteString takeMin() {
        int minIdx = -1;
        ByteString minLine = null;
        for (int i = 0; i < lines.length; i++) {
            ByteString line = peekLine(i);
            if (line == null) {
                continue;
            }
            if (minLine == null || streamCmp.compare(line, minLine) < 0) {
                minLine = line;
                minIdx = i;
            }
        }
        if (minLine == null) {
            return null;
        }
        takeNext[minIdx] = true;
        return minLine;
    }

    public ByteStreamMerger withStreamComparator(Comparator<ByteString> streamCmp) {
        this.streamCmp = streamCmp;
        return this;
    }

    public ByteStreamMerger withRecordComparator(Comparator<ByteString> recordCmp) {
        this.recordCmp = recordCmp;
        return this;
    }

    public void mergeTwo(BiConsumer<ByteString, ByteString> consumer) {
        merge(arr -> consumer.accept(arr[0], arr[1]));
    }

    public void mergeThree(ByteStringTriConsumer consumer) {
        merge(arr -> consumer.accept(arr[0], arr[1], arr[2]));
    }

    public static ByteStreamMerger of(Supplier<ByteString> one, Supplier<ByteString> two) {
        return new ByteStreamMerger(one, two);
    }

    public static ByteStreamMerger of(Supplier<ByteString> one, Supplier<ByteString> two, Supplier<ByteString> three) {
        return new ByteStreamMerger(one, two, three);
    }

    public static ByteStreamMerger of(Supplier<ByteString>... in) {
        return new ByteStreamMerger(in);
    }

    public static Supplier<ByteString> seq(ByteString ...arr) {
        return new Supplier<ByteString>() {
            int i = 0;

            @Override
            public ByteString get() {
                return i < arr.length ? arr[i++] : null;
            }
        };
    }

    public static Supplier<ByteString> seqIdx(ByteString string, long[] idxs) {
        return new Supplier<ByteString>() {
            int i = 0;

            @Override
            public ByteString get() {
                return i < idxs.length ?
                        string.substringIdx(idxs[i++]) :
                        null;
            }
        };
    }


    public interface ByteStringTriConsumer {
        void accept(ByteString a, ByteString b, ByteString c);
    }
}
