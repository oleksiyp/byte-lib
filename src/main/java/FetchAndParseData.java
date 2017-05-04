import byte_lib.Progress;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.squareup.okhttp.OkHttpClient;
import wikipageviews.PageView;
import wikipageviews.PageViewRecord;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static byte_lib.io.ByteFiles.printStream;
import static download.DumpWikimediaPageViews.fromMarch2015;

public class FetchAndParseData {

    public static final int K = 100;

    public static void main(String[] args) throws InterruptedException {
        Progress progress = Progress.toConsole(System.out);

        PageView.initLookups(progress);

        OkHttpClient downloadClient = new OkHttpClient();
        List<PageView> pageViews = fromMarch2015().getPageViews();

        Map<String, List<PageView>> pageViewsPerDay =
                pageViews
                        .stream()
                        .collect(Collectors.groupingBy(PageView::getDay));

        for (PageView pageView : pageViews) {
            try {
                pageView.setProgress(progress)
                        .download(downloadClient)
                        .writeTopToJson(K)
                        .discardContent();

                String day = pageView.getDay();
                List<PageView> grouped = pageViewsPerDay.get(day);
                if (grouped.stream().allMatch(PageView::hasTopRecords)) {
                    writeGroupedPerDay(progress,
                            grouped,
                            "daily/" + day + ".json");
                }
            } catch (IOException e) {
                // skip
            }
        }
    }

    private static void writeGroupedPerDay(Progress progress, List<PageView> grouped, String fileName) throws IOException {
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
                .limit(K)
                .collect(Collectors.toList());

        try (PrintStream out = printStream(fileName, progress)) {
            new ObjectMapper().writeValue(out, topPerDay);
        }
    }

}
