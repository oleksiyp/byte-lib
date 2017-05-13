package dbpedia;

import byte_lib.hashed.IdxByteStringMap;
import byte_lib.hashed.IdxMapper;
import byte_lib.io.ByteFiles;
import byte_lib.string.ByteString;
import byte_lib.string.ByteStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.function.Consumer;

import static byte_lib.io.ByteFiles.readAll;
import static byte_lib.string.ByteString.NEW_LINE;
import static byte_lib.string.ByteString.SEPARATOR;
import static byte_lib.string.ByteString.bs;

public class ImagesLookup {
    private static final Logger LOG = LoggerFactory.getLogger(ImagesLookup.class);

    public static final ByteString DBPEDIA_THUMBNAIL = bs("http://dbpedia.org/ontology/thumbnail");
    public static final ByteString FOAF_DEPICTION = bs("http://xmlns.com/foaf/0.1/depiction");

    private final File imagesData;
    private final File depictionFile;
    private final File thumbnailFile;

    private IdxByteStringMap thumbnailMap;
    private IdxByteStringMap depictionMap;

    public ImagesLookup(File imagesData, File depictionFile, File thumbnailFile) {
        this.imagesData = imagesData;
        this.depictionFile = depictionFile;
        this.thumbnailFile = thumbnailFile;
    }


    @PostConstruct
    public ImagesLookup init() {
        if (!depictionFile.isFile()
            || !thumbnailFile.isFile()) {
            LOG.info("Parsing {} data", imagesData);
            parseData();
        }

        LOG.info("Loading thumbnail info");
        thumbnailMap = new IdxByteStringMap(readAll(thumbnailFile),
                NEW_LINE,
                IdxMapper::firstTwoFields,
                IdxMapper::thirdField);

        LOG.info("Loading depiction info");
        depictionMap = new IdxByteStringMap(readAll(depictionFile),
                NEW_LINE,
                IdxMapper::firstTwoFields,
                IdxMapper::thirdField);

        return this;
    }

    private void parseData() {
        List<DbpediaFile> files = DbpediaFile.dirFiles(imagesData);

        try (PrintStream thumbnail = ByteFiles.printStream(thumbnailFile);
             PrintStream depiction = ByteFiles.printStream(depictionFile)) {

            Consumer<DbpediaTuple> parser = recordParser(thumbnail, depiction);

            files.forEach(file ->
                    file.reportNFile()
                            .recodeSnappy()
                            .readRecords(parser));
        }
    }

    private Consumer<DbpediaTuple> recordParser(PrintStream thumbnail, PrintStream depiction) {
        return record -> {
            ByteString predicate = record.getPredicate();

            if (DBPEDIA_THUMBNAIL.equals(predicate)) {
                writeImageUrl(thumbnail, record);
            } else if (FOAF_DEPICTION.equals(predicate)) {
                writeImageUrl(depiction, record);
            }
        };
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

    public ByteString getThumbnial(ByteString lang, ByteString resource) {
        return getThumbnial(
                new ByteStringBuilder()
                        .append(lang)
                        .append(SEPARATOR)
                        .append(resource)
                        .build());
    }

    public ByteString getThumbnial(ByteString langResource) {
        return thumbnailMap.get(langResource);
    }

    public ByteString getDepiction(ByteString lang, ByteString resource) {
        return getDepiction(
                new ByteStringBuilder()
                        .append(lang)
                        .append(SEPARATOR)
                        .append(resource)
                        .build());
    }

    public ByteString getDepiction(ByteString langResource) {
        return depictionMap.get(langResource);
    }

}