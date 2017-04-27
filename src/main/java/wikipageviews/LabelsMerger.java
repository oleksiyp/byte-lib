package wikipageviews;

import byte_lib.ByteStreamMerger;
import byte_lib.ByteString;
import byte_lib.ByteStringInputStream;
import byte_lib.Progress;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static byte_lib.ByteStreamMerger.seq;
import static byte_lib.ByteString.NEW_LINE;
import static byte_lib.ByteString.load;
import static byte_lib.ByteStringInputStream.file;
import static java.util.Comparator.comparing;

public class LabelsMerger {
    public static void main(String[] args) throws IOException {
        Progress progress = Progress.toConsole(System.out);

        AtomicInteger b = new AtomicInteger();
        ByteString p = load("pageviews-20170418-120000.gz", progress);
        System.out.println("Splitting");
        ByteString[] pageview = p.split(NEW_LINE);
        System.out.println("Sorting");
        Arrays.sort(pageview);

        System.out.println("Merging");
        try (ByteStringInputStream labels = file("labels2.txt.gz")) {
            ByteStreamMerger.of(seq(pageview), labels::nextLine)
                    .withRecordComparator(comparing(ByteString::firstTwoFields))
                    .mergeTwo((pv, label) -> b.incrementAndGet());
        }

        System.out.println(b);
    }
}
