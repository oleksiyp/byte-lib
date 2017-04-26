package wikipageviews;

import byte_lib.ByteStreamMerger;
import byte_lib.ByteString;
import byte_lib.ByteStringInputStream;

import java.io.IOException;
import java.util.Comparator;

import static byte_lib.ByteStringInputStream.file;
import static java.lang.Integer.MAX_VALUE;
import static java.util.Comparator.comparing;

public class LabelsMerger {
    public static void main(String[] args) throws IOException {
        try (ByteStringInputStream pageviews = file("pageviews-20170418-120000.gz");
             ByteStringInputStream labels = file("labels2.txt.gz")) {

            ByteStreamMerger.of(pageviews, labels)
                    .withRecordComparator(comparing(ByteString::firstTwoFields))
                    .mergeTwo((pageview, label) -> {
                System.out.println(pageview.field(2) + " " +
                            label.fields(2, MAX_VALUE));
            });
        }
    }
}
