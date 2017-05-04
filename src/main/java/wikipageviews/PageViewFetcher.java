package wikipageviews;

import byte_lib.Progress;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.squareup.okhttp.OkHttpClient;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static byte_lib.io.ByteFiles.printStream;
import static download.WikimediaDataSet.fromMarch2015;
import static java.util.stream.Collectors.toList;

public class PageViewFetcher {

    public static final int K = 100;
    private final OkHttpClient downloadClient;
    private Progress progress;
    private int topK;

    public PageViewFetcher(OkHttpClient downloadClient, Progress progress, int topK) {
        this.downloadClient = downloadClient;
        this.progress = progress;
        this.topK = topK;
    }

    public static void main(String[] args) throws InterruptedException {
        Progress progress = Progress.toConsole(System.out);

        PageView.initLookups(progress);

        OkHttpClient downloadClient = new OkHttpClient();
        List<PageView> pageViews = fromMarch2015().getPageViews();

        PageViewFetcher fetcher = new PageViewFetcher(downloadClient, progress, K);

        Map<String, List<PageView>> pageViewsPerDay =
                pageViews
                        .stream()
                        .collect(Collectors.groupingBy(PageView::getDay));

        List<String> days = pageViewsPerDay.keySet()
                .stream()
                .sorted(Comparator.reverseOrder())
                .collect(toList());

        for (String day : days) {
            try {
                fetcher.parseDay(pageViewsPerDay.get(day), "daily/" + day + ".json");
            } catch (IOException e) {
                // skip
            }
        }
    }

    private void parseFile(PageView pageView) {
        try {
            pageView.setProgress(progress)
                    .download(downloadClient)
                    .writeTopToJson(topK);
        } catch (IOException err) {
            throw new IOError(err);
        }
    }

    private void parseDay(List<PageView> pageViews, String fileName) throws IOException {
        pageViews.forEach(this::parseFile);

        aggregateDayTop(pageViews, fileName);
    }

    private void aggregateDayTop(List<PageView> grouped, String fileName) throws IOException {
        File file = new File(fileName);
        if (file.isFile()) {
            return;
        }
        file.getParentFile().mkdirs();
        List<PageViewRecord> topPerDay = grouped.stream()
                .map(PageView::getTopRecords)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(
                        PageViewRecord::getLangResource,
                        Function.identity(),
                        PageViewRecord::mergeTwoPageViews))
                .values()
                .stream()
                .sorted(Comparator.comparing(PageViewRecord::getScore).reversed())
                .limit(topK)
                .collect(toList());

        try (PrintStream out = printStream(fileName, progress)) {
            new ObjectMapper().writeValue(out, topPerDay);
        } catch (IOException ex) {
            throw new IOError(ex);
        }
    }

}
