package dbpedia;

import byte_lib.hashed.IdxByteStringMap;
import byte_lib.hashed.IdxMapper;
import byte_lib.string.ByteString;
import byte_lib.string.ByteStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.PrintStream;
import java.util.List;

import static byte_lib.io.ByteFiles.printStream;
import static byte_lib.io.ByteFiles.readAll;
import static byte_lib.string.ByteString.NEW_LINE;
import static byte_lib.string.ByteString.bs;

public class LabelsLookup {
    private static final Logger LOG = LoggerFactory.getLogger(LabelsLookup.class);

    private static final ByteString RDF_SCHEMA_LABEL = bs("http://www.w3.org/2000/01/rdf-schema#label");

    private final File labelsFile;
    private final File labelsData;

    private IdxByteStringMap labelsMap;

    public LabelsLookup(File labelsData, File labelsFile) {
        this.labelsFile = labelsFile;
        this.labelsData = labelsData;
    }

    @PostConstruct
    public void init() {
        if (!labelsFile.isFile()) {
            LOG.info("Parsing {} data", labelsData);
            parseData();
        }

        LOG.info("Loading labels info");
        labelsMap = new IdxByteStringMap(readAll(labelsFile),
                NEW_LINE,
                IdxMapper::firstTwoFields,
                IdxMapper::thirdField);
    }


    private void parseData() {
        List<DbpediaFile> files = DbpediaFile.dirFiles(
                labelsData
        );

        try (PrintStream out = printStream(labelsFile)) {
            files.forEach(file ->
                    file.reportNFile()
                            .recodeSnappy()
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
        ByteString ret = labelsMap.get(langResource);
        if (ret == null) {
            return resourceToLabel(langResource.secondField());
        }
        return ret;
    }

    public ByteString getLabel(ByteString lang, ByteString resource) {
        if (labelsMap.isEmpty()) {
            return resourceToLabel(resource);
        }
        return getLabel(
                new ByteStringBuilder(lang.length() + resource.length() + 1)
                        .append(lang)
                        .append((byte) ' ')
                        .append(resource)
                        .build());
    }
}
