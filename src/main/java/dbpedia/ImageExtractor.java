package dbpedia;

import byte_lib.ByteFiles;
import byte_lib.ByteString;
import byte_lib.Progress;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import static byte_lib.ByteString.bs;

public class ImageExtractor {
    public static final File IN_DIR = new File("data/images");
    public static final String OUT_THUMBNAIL = "thumbnail.txt.snappy";
    public static final String OUT_DEPICTION = "depiction.txt.snappy";

    public static final ByteString DBPEDIA_THUMBNAIL = bs("http://dbpedia.org/ontology/thumbnail");
    public static final ByteString FOAF_DEPICTION = bs("http://xmlns.com/foaf/0.1/depiction");

    public static void main(String[] args) throws InterruptedException, IOException {
        Progress progress = Progress.toConsole(System.out);

        List<DbpediaFile> files = DbpediaFile.dirFiles(IN_DIR, progress);

        try (PrintStream thumbnail = ByteFiles.printStream(OUT_THUMBNAIL, progress);
             PrintStream depiction = ByteFiles.printStream(OUT_DEPICTION, progress)) {

            files.forEach(file ->
                    file.reportNFile()
                            .countLines()
                            .readRecords((record) -> {
                                if (DBPEDIA_THUMBNAIL.equals(record.getPredicate())) {
                                    writeImageUrl(thumbnail, record);
                                } else if (FOAF_DEPICTION.equals(record.getPredicate())) {
                                    writeImageUrl(depiction, record);
                                }
                            })
            );
        }
//        FileSorter.sortFile(DEPICTION, DEPICTION_SORTED, progress);
//        FileSorter.sortFile(THUMBNAIL, THUMBNAIL_SORTED, progress);
    }

    private static void writeImageUrl(PrintStream out, DbpediaTuple record) {
        ByteString resourceLang = record.getDbpediaSubjectLang();
        ByteString resource = record.getDbpediaSubject();
        if (resource == null || resourceLang == null) {
            return;
        }
        resourceLang.writeTo(out);
        out.print(' ');
        resource.writeTo(out);
        out.print(' ');
        record.getObject().writeTo(out);
        out.println();
    }

}
