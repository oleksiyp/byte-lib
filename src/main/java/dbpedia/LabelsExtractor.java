package dbpedia;

import byte_lib.ByteString;
import byte_lib.Progress;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import static byte_lib.ByteString.bs;
import static byte_lib.ByteFiles.printStream;

public class LabelsExtractor {
    public static final File IN_DIR = new File("data/labels");
    public static final String OUT_FILE = "labels.txt.snappy";

    public static final ByteString SEPARATOR = bs(" ");
    public static final ByteString LABEL = bs("http://www.w3.org/2000/01/rdf-schema#label");
    public static final ByteString LABEL_SUBJECT_START = bs("http://");
    public static final ByteString LABEL_SUBJECT_MIDDLE = bs(".dbpedia.org/resource/");

    public static void main(String[] args) throws InterruptedException, IOException {
        Progress progress = Progress.toConsole(System.out);

        List<DbpediaFile> files = DbpediaFile.dirFiles(
                IN_DIR,
                progress);

        try (PrintStream out = printStream(OUT_FILE, progress)) {
            files.forEach(file ->
                    file.reportNFile()
                            .countLines()
                            .readRecords((record) -> writeLabel(out, record)));
        }

    }

    private static void writeLabel(PrintStream out, DbpediaTuple record) {
        if (!LABEL.equals(record.getPredicate())) {
            return;
        }

        ByteString resourceLang = record.getDbpediaSubjectLang();
        ByteString resource = record.getDbpediaSubject();
        if (resource == null || resourceLang == null) {
            return;
        }
        resourceLang.writeTo(out);
        out.print(' ');
        resource.writeTo(out);
        out.print(' ');
        record.getObjectLang().writeTo(out);
        out.print(' ');
        record.getObject().writeTo(out);
        out.println();
    }

}
