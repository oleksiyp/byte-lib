package byte_lib;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Comparator.comparing;

public class ByteStreamMerger {
    private final ByteStringInputStream[] in;
    private final boolean[] takeNext;
    private final ByteString[] lines;
    private Comparator<ByteString> streamCmp;
    private Comparator<ByteString> recordCmp;

    ByteStreamMerger(ByteStringInputStream ...in) {
        this.in = in;
        lines = new ByteString[in.length];
        takeNext = new boolean[in.length];
        Arrays.fill(takeNext, true);
        streamCmp = Comparator.naturalOrder();
        recordCmp = comparing(ByteString::firstField);
    }

    public ByteString peekLine(int idx) {
        if (takeNext[idx]) {
            lines[idx] = in[idx].nextLine();
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
        for (int i = 0; i < lines.length - 1; i++) {
            if (lines[i] == null || lines[i + 1] == null)  {
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

    public static ByteStreamMerger of(ByteStringInputStream ...in) {
        return new ByteStreamMerger(in);
    }
}
