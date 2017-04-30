package wikipageviews;

import byte_lib.*;
import byte_lib.sort.LongTimSort;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static byte_lib.ByteStreamMerger.seqIdx;
import static byte_lib.ByteString.NEW_LINE;
import static java.util.Comparator.comparing;

public class PageViewDataEnrich {
    public static final String DEPICTION_SORTED = "depiction_sorted.txt.snappy";

    public static void main(String[] args) throws IOException {
        Progress progress = Progress.toConsole(System.out);

        ByteString pageviews = ByteFiles.readAll("pageviews-20170418-120000.gz", progress);

        System.out.println("Splitting");
        long[] idxs = pageviews.splitIdx(NEW_LINE);
        System.out.println("Sorting");
        LongTimSort.sort(idxs, pageviews::compareByIdx);

        System.out.println("Merging");
        AtomicInteger b = new AtomicInteger();
        try (ByteStringInputStream depictions = ByteFiles.inputStream("labels_sorted.txt.snappy")) {
            Supplier<ByteString> first = seqIdx(pageviews, idxs);
            Supplier<ByteString> second = depictions::nextLine;
            ByteStreamMerger.of(first, second)
                    .withRecordComparator(comparing(ByteString::firstTwoFields))
                    .mergeTwo((pv, depiction) -> b.incrementAndGet());
        }

        System.out.println(b + " " + idxs.length);
    }

}
