package wikipageviews;

import byte_lib.ByteString;
import byte_lib.ByteStringFilter;
import byte_lib.ByteStringInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static byte_lib.ByteString.bs;

public class InterlinksMapExtractor {
    public static final ByteString COMMENT_START = bs("#");
    public static final ByteString SEPARATOR = bs(":");

    private static boolean isCommentLine(ByteString s) {
        return s.trim().startsWith(COMMENT_START);
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        System.out.println("Counting lines");
        int cnt = tableIn().countLines();
        System.out.println("Reading interlinks " + cnt);
        Progress progress = new Progress(cnt);

        ByteStringFilter filter = new ByteStringFilter(cnt);

        System.out.println("Writing table2.txt.gz");

        int dups = 0, parsing = 0, comments = 0;
        try (ByteStringInputStream in = tableIn();
             PrintStream out = table2Out()) {

            ByteString line;
            while ((line = in.nextLine()) != null) {
                progress.progress();
                if (isCommentLine(line)) {
                    comments++;
                    continue;
                }

                InterlinkRecord record = InterlinkRecordParser.parse(line);

                if (record == null) {
                    parsing++;
                    continue;
                }

                if (!filter.add(record.getLang(), record.getIntlTopic())) {
                    dups++;
                    continue;
                }

                record.getLang().writeTo(out);
                out.print(":");
                record.getIntlTopic().writeTo(out);
                out.print(" ");
                record.getTopic().writeTo(out);
                out.println();
            }
        }
        System.out.println("Duplicates " + dups + " comments " + comments + " parsing " + parsing);
    }


    private static PrintStream table2Out() throws IOException {
        return new PrintStream(
                new GZIPOutputStream(
                        new FileOutputStream("table2.txt.gz")));
    }

    private static ByteStringInputStream tableIn() throws IOException {
        return new ByteStringInputStream(
                new BZip2CompressorInputStream(
                        new FileInputStream("interlanguage_links_en.tql.bz2")));
    }
}
