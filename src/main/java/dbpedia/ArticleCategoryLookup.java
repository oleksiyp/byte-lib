package dbpedia;

import byte_lib.hashed.IdxByteStringMap;
import byte_lib.hashed.IdxByteStringMultiMap;
import byte_lib.hashed.IdxMapper;
import byte_lib.io.ByteFiles;
import byte_lib.io.ByteStringInputStream;
import byte_lib.ordered.FileSorter;
import byte_lib.string.ByteString;
import byte_lib.string.ByteStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import static byte_lib.io.ByteFiles.printStream;
import static byte_lib.io.ByteFiles.readAll;
import static byte_lib.string.ByteString.EMPTY;
import static byte_lib.string.ByteString.NEW_LINE;
import static byte_lib.string.ByteString.bs;
import static java.lang.Integer.MAX_VALUE;
import static java.util.Arrays.asList;

public class ArticleCategoryLookup {
    private static final Logger LOG = LoggerFactory.getLogger(ArticleCategoryLookup.class);

    private static final ByteString PURL_DC_SUBJECT = bs("http://purl.org/dc/terms/subject");
    public static final ByteString SEPARATOR = bs(",");

    private final File file;
    private final File data;
    private CategoryLabelsLookup categoryLabelsLookup;

    private IdxByteStringMap categoryMap;

    public ArticleCategoryLookup(File data, File file, CategoryLabelsLookup categoryLabelsLookup) {
        this.file = file;
        this.data = data;
        this.categoryLabelsLookup = categoryLabelsLookup;
    }

    @PostConstruct
    public void init() {
        if (!file.isFile()) {
            LOG.info("Parsing {} data", data);
            parseData();
        }

        File newFile1 = new File(file.getAbsolutePath().replace(".snappy", "2.snappy"));
        LOG.info("Recoding " + newFile1);
        if (!newFile1.isFile()) {
            try {
                FileSorter.sortFile(file.getAbsolutePath(), newFile1.getAbsolutePath());
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        File newFile = new File(file.getAbsolutePath().replace(".snappy", "3.snappy"));
        LOG.info("Recoding " + newFile);
        if (!newFile.isFile()) {
            try (ByteStringInputStream in = ByteFiles.inputStream(newFile1);
                 PrintStream out = ByteFiles.printStream(newFile)) {
                ByteString []prevRes = new ByteString[1];
                in.readLines(line -> {
                    ByteString nowRes = line.firstTwoFields();
                    if (prevRes[0] == null || !prevRes[0].equals(nowRes)) {
                        if (prevRes[0] != null) {
                            out.print('\n');
                        }
                        nowRes.writeTo(out);
                        out.print(' ');
                        line.fields(2, MAX_VALUE).writeTo(out);
                        prevRes[0] = nowRes.copyOf();
                    } else {
                        out.print(',');
                        line.fields(2, MAX_VALUE).writeTo(out);
                    }
                });
                if (prevRes[0] != null) {
                    out.print('\n');
                }
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        LOG.info("Loading article categories info");
        categoryMap = new IdxByteStringMap(
                readAll(newFile),
                NEW_LINE,
                IdxMapper::firstTwoFields,
                IdxMapper.fields(2, MAX_VALUE));
    }


    private void parseData() {
        List<DbpediaFile> files = DbpediaFile.dirFiles(
                data
        );

        try (PrintStream out = printStream(file)) {
            files.forEach(file ->
                    file.reportNFile()
                            .recodeSnappy()
                            .countLines()
                            .readRecords((record) -> writeArticleCategory(out, record)));
        }
    }

    private void writeArticleCategory(PrintStream out, DbpediaTuple record) {
        if (!PURL_DC_SUBJECT.equals(record.getPredicate())) {
            return;
        }

        ByteString resourceLang = record.getDbpediaSubjectLang();
        ByteString resource = record.getDbpediaSubject();
        if (resource == null || resourceLang == null) {
            return;
        }

        ByteString objResourceLang = record.getDbpediaObjectLang();
        ByteString objResource = record.getDbpediaObject();
        if (objResource == null || objResourceLang == null) {
            return;
        }

        ByteString category = categoryLabelsLookup.getLabel(
                objResourceLang,
                objResource);
        if (category == null) {
            return;
        }

        resourceLang.writeTo(out);
        out.print(' ');
        resource.writeTo(out);
        out.print(' ');
        objResourceLang.writeTo(out);
        out.print(' ');
        category.writeTo(out);
        out.println();
    }


    public List<ByteString> getCategory(ByteString langResource) {
        ByteString cats = categoryMap.get(langResource);
        if (cats == null) {
            cats = EMPTY;
        }
        return asList(cats.split(SEPARATOR));
    }

    public List<ByteString> getCategory(ByteString lang, ByteString resource) {
        return getCategory(
                new ByteStringBuilder(lang.length() + resource.length() + 1)
                        .append(lang)
                        .append((byte) ' ')
                        .append(resource)
                        .build());
    }

    public static void main(String[] args) {
        CategoryLabelsLookup lookup = new CategoryLabelsLookup(
                new File("data/category_labels"),
                new File("parsed/category_labels.txt.snappy"));
        lookup.init();

        ArticleCategoryLookup articleCategoryLookup = new ArticleCategoryLookup(
                new File("data/article_categories"),
                new File("parsed/article_categories.txt.snappy"),
                lookup
        );
        articleCategoryLookup.init();
        System.out.println(articleCategoryLookup.categoryMap.size());
    }
}
