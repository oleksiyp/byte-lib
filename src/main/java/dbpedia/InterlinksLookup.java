package dbpedia;

import byte_lib.ByteFiles;
import byte_lib.ByteString;
import byte_lib.ByteStringMap;
import byte_lib.Progress;

import java.io.File;
import java.util.*;

import static byte_lib.ByteFiles.loadMap;
import static byte_lib.ByteFiles.readAll;
import static byte_lib.ByteString.NEW_LINE;
import static byte_lib.ByteString.bs;
import static java.util.Arrays.asList;

public class InterlinksLookup {
    public static final File IN_DATA = new File("data/interlinks/interlanguage_links_en.tql.bz2");

    public static final File MAIN_PAGE_TXT = new File("parsed/main_page.txt");
    public static final File SPECIAL_TXT = new File("parsed/special_page.txt");
    public static final File TEMPLATE_TXT = new File("parsed/template_page.txt");

    public static final ByteString MAIN_PAGE_RESOURCE = bs("http://dbpedia.org/resource/Main_Page");
    public static final ByteString OWL_SAME_AS = bs("http://www.w3.org/2002/07/owl#sameAs");

    public static final ByteString EN = bs("en");
    public static final ByteString MAIN_PAGE = bs("Main_Page");

    public static final ByteString TEMPLATE_PREFIX = bs("http://dbpedia.org/resource/Template:");
    public static final ByteString SPECIAL_PREFIX = bs("http://dbpedia.org/resource/Special:");
    public static final ByteString COLON = bs(":");

    private ByteStringMap<ByteString> mainPages;
    private List<ByteString> specialPages;
    private List<ByteString> templatePages;

    public static InterlinksLookup INTERLINKS;

    public static InterlinksLookup init(Progress progress) {
        if (INTERLINKS == null) {
            INTERLINKS = new InterlinksLookup();
            INTERLINKS.init0(progress);
        }

        return INTERLINKS;
    }

    public static InterlinksLookup init() {
        return init(null);
    }

    private void init0(Progress progress) {
        if (!load(progress)) {
            parseData(progress);
            save(progress);
        }
    }

    InterlinksLookup() {
        mainPages = new ByteStringMap<>();
        specialPages = new ArrayList<>();
        templatePages = new ArrayList<>();
    }

    private boolean load(Progress progress) {
        if (!MAIN_PAGE_TXT.isFile() ||
                !TEMPLATE_TXT.isFile() ||
                !SPECIAL_TXT.isFile()) {
            return false;
        }

        try {
            mainPages = loadMap(MAIN_PAGE_TXT, progress);
            ByteFiles.loadCollection(SPECIAL_TXT, specialPages, progress);
            ByteFiles.loadCollection(TEMPLATE_TXT, templatePages, progress);
            Collections.sort(specialPages);
            Collections.sort(templatePages);
        } catch (Exception ex) {
            return false;
        }

        return true;
    }

    private void save(Progress progress) {
        MAIN_PAGE_TXT.getParentFile().mkdirs();
        SPECIAL_TXT.getParentFile().mkdirs();
        TEMPLATE_TXT.getParentFile().mkdirs();

        ByteFiles.writeMap(MAIN_PAGE_TXT, mainPages);
        ByteFiles.writeCollection(SPECIAL_TXT, specialPages);
        ByteFiles.writeCollection(TEMPLATE_TXT, templatePages);
    }

    private void parseData(Progress progress) {
        new DbpediaFile(IN_DATA)
                .setProgress(progress)
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
        } else if (record.getSubject().contains(COLON)) {
            // TODO remove
            System.out.println(record.getSubject().toString());
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
