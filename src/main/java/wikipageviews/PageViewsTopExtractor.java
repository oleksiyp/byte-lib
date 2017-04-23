package wikipageviews;

import byte_lib.ByteString;
import byte_lib.ByteStringMap;
import byte_lib.ByteStringInputStream;

import java.io.*;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.zip.GZIPInputStream;

import static byte_lib.ByteString.bs;

public class PageViewsTopExtractor {
    public static final ByteString SEPARATOR = bs(" ");
    public static final ByteString SEPARATOR2 = bs(":");

    public static void main(String[] args) throws InterruptedException, IOException {
        int cnt = tableIn().countLines();
        System.out.println("Reading table " + cnt);
        Progress progress = new Progress(cnt);
        Map<ByteString, ByteString> interlinks = new ByteStringMap<>(cnt);

        try (ByteStringInputStream in = tableIn()) {
            ByteString line;
            while ((line = in.nextLine()) != null) {
                progress.progress();
                ByteString[] arr = line.split(SEPARATOR);
                interlinks.put(arr[0].copyOf(false), arr[1].copyOf(false));
            }
        }

        PriorityQueue<PageViewRecord> topK = new PriorityQueue<>(
                Comparator.comparingInt(PageViewRecord::getViews));

        int k = 10000;
        cnt = pageviewIn().countLines();
        System.out.println("Reading pageviews " + cnt);
        progress = new Progress(cnt);
        try (ByteStringInputStream in = pageviewIn()) {
            ByteString line;
            while ((line = in.nextLine()) != null) {
                progress.progress();
                PageViewRecord record = PageViewRecord.valueOf(line);
                topK.add(record);
                if (topK.size() > k) {
                    topK.remove();
                }
            }
        }

        try (PrintStream out = new PrintStream("result.txt")) {
            while (topK.isEmpty()) {
                PageViewRecord record = topK.remove();
                ByteString page = record.getProject()
                        .append(SEPARATOR2)
                        .append(record.getTitle());

                ByteString topic = interlinks.get(page);
                if (topic != null) {
                    out.println(topic + " " + record.getViews());
                }
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
