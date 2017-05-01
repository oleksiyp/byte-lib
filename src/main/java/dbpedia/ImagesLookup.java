package dbpedia;

import byte_lib.*;

import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static byte_lib.ByteFiles.readAll;
import static byte_lib.ByteString.NEW_LINE;
import static byte_lib.ByteString.SEPARATOR;
import static byte_lib.ByteString.bs;

public class ImagesLookup {
    public static final File IN_DATA = new File("data/images");

    public static final File DEPICTION_TXT = new File("parsed/depiction.txt.snappy");
    public static final File THUMBNAIL_TXT = new File("parsed/thumbnail.txt.snappy");

    public static final ByteString DBPEDIA_THUMBNAIL = bs("http://dbpedia.org/ontology/thumbnail");
    public static final ByteString FOAF_DEPICTION = bs("http://xmlns.com/foaf/0.1/depiction");

    public static ImagesLookup IMAGES;

    private IdxByteStringMap thumbnailMap;
    private IdxByteStringMap depictionMap;

    public ImagesLookup() {
    }


    public static ImagesLookup init(Progress progress) {
        if (IMAGES == null) {
            IMAGES = new ImagesLookup();
            IMAGES.init0(progress);
        }

        return IMAGES;

    }

    private void init0(Progress progress) {
        if (!DEPICTION_TXT.isFile()
            || !THUMBNAIL_TXT.isFile()) {
            parseData(progress);
        }

        thumbnailMap = new IdxByteStringMap(readAll(THUMBNAIL_TXT, progress),
                NEW_LINE,
                IdxMapper::firstTwoFields,
                IdxMapper::thirdField);

        depictionMap = new IdxByteStringMap(readAll(DEPICTION_TXT, progress),
                NEW_LINE,
                IdxMapper::firstTwoFields,
                IdxMapper::thirdField);
    }

    private void parseData(Progress progress) {
        List<DbpediaFile> files = DbpediaFile.dirFiles(IN_DATA, progress);

        try (PrintStream thumbnail = ByteFiles.printStream(THUMBNAIL_TXT, progress);
             PrintStream depiction = ByteFiles.printStream(DEPICTION_TXT, progress)) {

            Consumer<DbpediaTuple> parser = recordParser(thumbnail, depiction);

            files.forEach(file ->
                    file.reportNFile()
                            .recodeSnappy()
                            .countLines()
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