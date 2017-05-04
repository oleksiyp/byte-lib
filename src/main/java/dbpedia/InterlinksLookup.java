package dbpedia;

import byte_lib.io.ByteFiles;
import byte_lib.string.ByteString;
import byte_lib.hashed.ByteStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.*;

import static byte_lib.io.ByteFiles.loadMap;
import static byte_lib.io.ByteFiles.readAll;
import static byte_lib.string.ByteString.bs;

public class InterlinksLookup {
    private static final Logger LOG = LoggerFactory.getLogger(LabelsLookup.class);

    public static final ByteString MAIN_PAGE_RESOURCE = bs("http://dbpedia.org/resource/Main_Page");
    public static final ByteString OWL_SAME_AS = bs("http://www.w3.org/2002/07/owl#sameAs");

    public static final ByteString EN = bs("en");
    public static final ByteString MAIN_PAGE = bs("Main_Page");

    public static final ByteString TEMPLATE_PREFIX = bs("http://dbpedia.org/resource/Template:");
    public static final ByteString SPECIAL_PREFIX = bs("http://dbpedia.org/resource/Special:");
    public static final ByteString COLON = bs(":");
    private final File mainPagesFile;
    private final File templatePagesFile;
    private final File specialPagesFile;
    private final File interlinksData;

    private ByteStringMap<ByteString> mainPages;
    private Set<ByteString> specialPages;
    private Set<ByteString> templatePages;

    @PostConstruct
    public void init() {
        if (!load()) {
            parseData();
            save();
        }
    }

    public InterlinksLookup(File interlinksData, File mainPagesFile, File templatePagesFile, File specialPagesFile) {
        mainPages = new ByteStringMap<>();
        specialPages = new HashSet<>();
        templatePages = new HashSet<>();
        this.mainPagesFile = mainPagesFile;
        this.templatePagesFile = templatePagesFile;
        this.specialPagesFile = specialPagesFile;
        this.interlinksData = interlinksData;
    }

    private boolean load() {
        if (!mainPagesFile.isFile() ||
                !templatePagesFile.isFile() ||
                !specialPagesFile.isFile()) {
            return false;
        }

        LOG.info("Loading {}, {}, {}",
                mainPagesFile,
                specialPagesFile,
                templatePagesFile);
        try {
            mainPages = loadMap(mainPagesFile);
            ByteFiles.loadCollection(specialPagesFile, specialPages);
            ByteFiles.loadCollection(templatePagesFile, templatePages);
        } catch (Exception ex) {
            return false;
        }

        return true;
    }

    private void save() {
        mainPagesFile.getParentFile().mkdirs();
        specialPagesFile.getParentFile().mkdirs();
        templatePagesFile.getParentFile().mkdirs();

        ByteFiles.writeMap(mainPagesFile, mainPages);
        ByteFiles.writeCollection(specialPagesFile, specialPages);
        ByteFiles.writeCollection(templatePagesFile, templatePages);
    }

    private void parseData() {
        LOG.info("Parsing {} data", interlinksData);
        new DbpediaFile(interlinksData)
                .recodeSnappy()
                .countLines()
                .readRecords(this::parseInterlinkRecord);

        mainPages.put(EN, MAIN_PAGE);
    }

    private void parseInterlinkRecord(DbpediaTuple record) {
        if (!OWL_SAME_AS.equals(record.getPredicate())) {
            return;
        }
        if (MAIN_PAGE_RESOURCE.equals(record.getSubject())) {
            ByteString obj = record.getDbpediaObject();
            ByteString lang = record.getDbpediaObjectLang();

            if (obj != null && lang != null) {
                mainPages.put(lang.copyOf(), obj.copyOf());
            }
        }
        if (record.getSubject().startsWith(SPECIAL_PREFIX)) {
            addPrefix(specialPages, record);
        } else if (record.getSubject().startsWith(TEMPLATE_PREFIX)) {
            addPrefix(templatePages, record);
        }
    }


    private void addPrefix(Collection<ByteString> set, DbpediaTuple record) {
        addPrefix(set, record.getDbpediaObject());
        addPrefix(set, record.getDbpediaSubject());
    }

    private void addPrefix(Collection<ByteString> set, ByteString str) {
        if (str == null) {
            return;
        }
        long idx = str.indexOf((byte) ':');
        if (idx == -1) {
            return;
        }
        set.add(str.substring(0, idx).copyOf());
    }

    public boolean isMainPage(ByteString lang, ByteString page) {
        return page.equals(mainPages.get(lang));
    }

    public boolean isSpecial(ByteString page) {
        return specialPages.contains(page.field(COLON, 0));
    }

    public boolean isTemplate(ByteString page) {
        return templatePages.contains(page.field(COLON, 0));
    }

    public boolean hasLang(ByteString lang) {
        return mainPages.containsKey(lang);
    }
}
