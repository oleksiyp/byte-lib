package wikipageviews;

import byte_lib.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.PriorityQueue;
import java.util.zip.GZIPInputStream;

import static byte_lib.ByteString.bs;
import static byte_lib.ByteStringInputStream.file;
import static dbpedia.Compressed.snappyPrintStream;
import static java.util.Comparator.comparingInt;

public class PageViewsTopExtractor {
    public static final ByteString SEPARATOR = bs(" ");
    public static final ByteString SEPARATOR2 = bs(":");
    public static final ByteString NEW_LINE = bs("\n");

    public static void main(String[] args) throws InterruptedException, IOException {
        PriorityQueue<PageViewRecord> topK = new PriorityQueue<>(
                comparingInt(PageViewRecord::getViews));

        int k = 10000;
        Progress progress = Progress.toConsole(System.out);

        ByteString pageviews = ByteString.load("pageviews-20170418-120000.gz", progress);

        MainPageRate mainPageRate = new MainPageRate();

        pageviews.iterate(NEW_LINE, (line)-> {
            PageViewRecord record = PageViewRecord.valueOf(line);
            topK.add(record);
            if (topK.size() > k) {
                topK.remove();
            }
        });

        try (PrintStream out = snappyPrintStream("result.txt.snappy", progress)) {
            while (!topK.isEmpty()) {
                PageViewRecord record = topK.remove();
                out.printf("%s %s %s %d %.2f%n",
                        record.getProject(),
                        record.getTitle(),
                        record.getConcept(),
                        record.getViews(),
                        record.getScore(mainPageRate).orElse(0));
            }
        }
    }

    private static ByteStringInputStream tableIn() throws IOException {
        return new ByteStringInputStream(
                new GZIPInputStream(
                        new FileInputStream("table.txt.gz")));
    }

    private static ByteStringInputStream pageviewIn() throws IOException {
        return new ByteStringInputStream(
                new GZIPInputStream(
                        new FileInputStream("pageviews-20170418-120000.gz")));
    }

}
