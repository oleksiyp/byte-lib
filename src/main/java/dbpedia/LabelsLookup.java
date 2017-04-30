package dbpedia;

import byte_lib.*;

import java.io.File;
import java.io.PrintStream;
import java.util.List;

import static byte_lib.ByteFiles.printStream;
import static byte_lib.ByteFiles.readAll;
import static byte_lib.ByteString.NEW_LINE;
import static byte_lib.ByteString.bs;

public class LabelsLookup {
    public static final File IN_DIR = new File("data/labels");
    public static final File LABELS_TXT = new File("parsed/labels.txt.snappy");

    private static final ByteString RDF_SCHEMA_LABEL = bs("http://www.w3.org/2000/01/rdf-schema#label");

    private static LabelsLookup LABELS;

    private IdxByteStringMap labelsMap;

    public static LabelsLookup init(Progress progress) {
        if (LABELS == null) {
            LABELS = new LabelsLookup();
            LABELS.init0(progress);
        }
        return LABELS;
    }

    private void init0(Progress progress) {
        if (!LABELS_TXT.isFile()) {
            parseData(progress);
        }

        labelsMap = new IdxByteStringMap(readAll(LABELS_TXT, progress),
                NEW_LINE,
                IdxMapper::firstTwoFields,
                IdxMapper::thirdField);
    }


    private void parseData(Progress progress) {
        List<DbpediaFile> files = DbpediaFile.dirFiles(
                IN_DIR,
                progress);

        try (PrintStream out = printStream(LABELS_TXT, progress)) {
            files.forEach(file ->
                    file.reportNFile()
                            .recodeSnappy()
                            .countLines()
                            .readRecords((record) -> writeLabel(out, record)));
        }
    }
    private void writeLabel(PrintStream out, DbpediaTuple record) {
        if (!RDF_SCHEMA_LABEL.equals(record.getPredicate())) {
            return;
        }

        ByteString resourceLang = record.getDbpediaSubjectLang();
        ByteString resource = record.getDbpediaSubject();
        if (resource == null || resourceLang == null) {
            return;
        }

        ByteString label = resourceToLabel(resource);
        if (record.getObject().equals(label)) {
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

    private ByteString resourceToLabel(ByteString resource) {
        ByteStringBuilder builder = new ByteStringBuilder();
        long len = resource.length();
        for (int i = 0; i < len; i++) {
            byte b = resource.byteAt(i);
            if (b == '_') {
                builder.append((byte) ' ');
            } else if (b == '%' && i + 2 < len) {
                int val = resource.substring(i + 1, i + 3).toInt(16);
                builder.append((byte) val);
                i += 2;
            } else {
                builder.append(b);
            }
        }
         return builder.build();
    }


    public ByteString getLabel(ByteString langResource) {
        return labelsMap.get(langResource);
    }

    public static void main(String[] args) {
        LabelsLookup.init(Progress.toConsole(System.out));
    }
}
