package dbpedia;

import byte_lib.ByteString;
import byte_lib.ByteStringFilter;
import byte_lib.Progress;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import static byte_lib.ByteString.bs;

public class InterlinksExtractor {
    public static final ByteString SEPARATOR = bs(" ");

    public static void main(String[] args) throws InterruptedException, IOException {
        File path = new File("data/interlinks");

        Progress progress = Progress.toConsole(System.out);

        List<DbpediaFile> interlinks = Stream.of(
                new File("data/interlinks/")
        )
//                Optional.ofNullable(path.listFiles()).orElse(new File[0]))
                .map(DbpediaFile::new)
                .peek((f) -> f.setProgress(progress))
                .limit(3)
                .collect(Collectors.toList());

        ByteStringFilter filter = ByteStringFilter.bloom(32, 32);

        try (PrintStream out = new PrintStream(
                new GZIPOutputStream(
                        new FileOutputStream("interlinks.txt.gz")))) {
            System.out.println("Writing interlinks.txt.gz");

            interlinks.forEach(dbpediaFile -> {
                System.out.println("Reading " + dbpediaFile.getFile().getName());
                dbpediaFile.readRecords((record) -> {
//                    if (!filter.add(record.getLang(), record.getIntlTopic())) {
//                        return;
//                    }
//
//                    record.getLang().writeTo(out);
//                    out.print(":");
//                    record.getIntlTopic().writeTo(out);
//                    out.print(" ");
//                    record.getTopic().writeTo(out);
//                    out.println();
                });
            });
        }
    }
}
