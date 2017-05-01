package wikipageviews;

import byte_lib.ByteFiles;
import byte_lib.ByteString;
import byte_lib.Progress;
import com.fasterxml.jackson.databind.ObjectMapper;
import dbpedia.ImagesLookup;
import dbpedia.InterlinksLookup;
import dbpedia.LabelsLookup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import static byte_lib.ByteString.bs;
import static dbpedia.ImagesLookup.IMAGES;
import static dbpedia.InterlinksLookup.INTERLINKS;
import static dbpedia.LabelsLookup.LABELS;
import static java.util.Comparator.comparingInt;

public class PageViewGenerateJson {
    public static final ByteString COLON = bs(":");
    private static final File OUT_FILE = new File("pageviews-20170418-120000.json");

    private MainPageRate rate;
    private Progress progress;
    private ByteString pageviews;
    public static final int K = 1000;

    public PageViewGenerateJson() {
        rate = new MainPageRate();
    }

    public static void main(String[] args) throws IOException {
        Progress progress = Progress.toConsole(System.out);
        List<PageViewRecord> records = new PageViewGenerateJson()
                .setProgress(progress)
                .readContent()
                .initLookups()
                .parseRecords(K);

        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(OUT_FILE, records);
    }

    private List<PageViewRecord> parseRecords(int k) {
        PriorityQueue<PageViewRecord> topK;
        topK = new PriorityQueue<>(
                comparingInt(PageViewRecord::getStatCounter));

        pageviews.iterate(ByteString.NEW_LINE, (pageview) -> {
            PageViewRecord record = parseRecord(pageview);
            if (record == null) {
                return;
            }

            topK.add(record);
            if (topK.size() > k) {
                topK.remove();
            }
        });

        List<PageViewRecord> records = new ArrayList<>();
        while (!topK.isEmpty()) {
            PageViewRecord record = topK.remove();
            record.calcScore(rate);
            records.add(record);
        }

        Collections.reverse(records);

        return records;
    }

    private PageViewGenerateJson initLookups() {
        InterlinksLookup.init(progress);
        ImagesLookup.init(progress);
        LabelsLookup.init(progress);
        return this;
    }

    private PageViewGenerateJson readContent() {
        if (pageviews == null) {
            pageviews = ByteFiles.readAll("pageviews-20170418-120000.gz", progress);
        }
        return this;
    }

    private PageViewGenerateJson setProgress(Progress progress) {
        this.progress = progress;
        return this;
    }

    private PageViewRecord parseRecord(ByteString pageview) {
        ByteString lang = pageview.firstField();
        if (!INTERLINKS.hasLang(lang)) {
            return null;
        }

        ByteString resource = pageview.field(1);
        int statCounter = pageview.field(2).toInt();
        if (INTERLINKS.isMainPage(lang, resource)) {
            rate.add(statCounter);
            return null;
        }

        if (INTERLINKS.isSpecial(resource) || INTERLINKS.isTemplate(resource)) {
            return null;
        }

        ByteString thumbnail = IMAGES.getThumbnial(pageview.firstTwoFields());
        ByteString depiction = IMAGES.getDepiction(pageview.firstTwoFields());
        ByteString label = LABELS.getLabel(pageview.firstTwoFields());

        if (thumbnail == null || depiction == null) {
            return null;
        }

        return new PageViewRecord(lang, resource, statCounter,
                thumbnail, depiction, label);
    }


}
