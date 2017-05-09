package dbpedia;

import byte_lib.hashed.IdxByteStringMultiMap;
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

public class ArticleCategoryLookup {
    private static final Logger LOG = LoggerFactory.getLogger(ArticleCategoryLookup.class);

    private static final ByteString PURL_DC_SUBJECT = bs("http://purl.org/dc/terms/subject");

    private final File file;
    private final File data;
    private CategoryLabelsLookup categoryLabelsLookup;

    private IdxByteStringMultiMap categoryMap;

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

        LOG.info("Loading article categories info");
        categoryMap = new IdxByteStringMultiMap(
                readAll(file),
                NEW_LINE,
                IdxMapper::firstTwoFields,
                IdxMapper.fields(2, 3));
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
        return categoryMap.get(langResource);
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
