package dbpedia;

import byte_lib.ByteString;
import byte_lib.Progress;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import static byte_lib.ByteString.bs;
import static dbpedia.LabelRecordParser.PARSER;

public class LabelsExtractor {
    public static final ByteString SEPARATOR = bs(" ");
    private static final ByteString NEW_LINE = bs("\n");

    public static void main(String[] args) throws InterruptedException, IOException {
        File path = new File("data/labels");

        Progress progress = Progress.toConsole(System.out);

        List<DbpediaFile> files = Stream.of(
                new File("labels_uk.tql.bz2")
        )
//                Optional.ofNullable(path.listFiles()).orElse(new File[0]))
                .map(DbpediaFile::new)
                .peek((f) -> f.setProgress(progress))
                .limit(3)
                .collect(Collectors.toList());

        try (PrintStream out = new PrintStream(
                new GZIPOutputStream(
                        new FileOutputStream("labels.txt.gz")))) {
            System.out.println("Writing labels.txt.gz");

            files.forEach(file ->
                    file.countLines().readRecords(PARSER::parse, (record) -> {
                record.writeTo(out);
            }));
        }

        ByteString labels = ByteString.load("labels.txt.gz", progress);
        ByteString[] lines = labels.split(NEW_LINE);
        Arrays.sort(lines);
        try (PrintStream out = new PrintStream(
                new GZIPOutputStream(
                        new FileOutputStream("labels2.txt.gz")))) {
            for (ByteString line : lines) {
                line.writeTo(out);
                out.println();
            }
        }
    }

}
