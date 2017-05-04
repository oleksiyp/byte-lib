package daily_service;

import download.WikimediaDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import wikipageviews.PageView;
import wikipageviews.PageViewFetcher;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;

public class DailyTopService {
    private final static Logger LOG = LoggerFactory.getLogger(DailyTopService.class);

    private final WikimediaDataSet dataSet;
    private PageViewFetcher fetcher;
    private Consumer<String> dailyNotifier;
    private PriorityExecutor perDayExecutor;

    public DailyTopService(WikimediaDataSet dataSet,
                           PageViewFetcher fetcher,
                           Consumer<String> dailyNotifier,
                           PriorityExecutor perDayExecutor) {
        this.dataSet = dataSet;
        this.fetcher = fetcher;
        this.dailyNotifier = dailyNotifier;

        this.perDayExecutor = perDayExecutor;
    }

    @PostConstruct
    public void init() {
        run();
    }

    @Scheduled(cron="0 5 * * * *")
    public void run() {
        List<PageView> fetchedPageViews = dataSet
                .fetchPageviews()
                .getPageViews();

        for (String day : daysHasMissingJsons(fetchedPageViews)) {
            List<PageView> pageViews = pageViewsForDay(fetchedPageViews, day);
            PageViewDailyFetchTask task = new PageViewDailyFetchTask(day, pageViews);
            perDayExecutor.execute(task);
        }
    }

    private List<PageView> pageViewsForDay(List<PageView> fetchedPageViews, String day) {
        return fetchedPageViews
                .stream()
                .collect(groupingBy(PageView::getDay))
                .get(day);
    }

    private List<String> daysHasMissingJsons(List<PageView> fetchedPageViews) {
        return fetchedPageViews
                .stream()
                .filter(PageView::hasNoJsonOut)
                .map(PageView::getDay)
                .collect(toSet())
                .stream()
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());
    }

    private class PageViewDailyFetchTask implements Runnable, Comparable<PageViewDailyFetchTask> {
        private final String day;
        private final List<PageView> pageViews;

        private PageViewDailyFetchTask(String day, List<PageView> pageViews) {
            this.day = day;
            this.pageViews = pageViews;
        }

        @Override
        public int compareTo(PageViewDailyFetchTask o) {
            return -day.compareTo(o.day);
        }

        @Override
        public void run() {
            try {
                LOG.info("Parsing day {}, {} files", day, pageViews.size());
                fetcher.parseDay(day, pageViews);
                fetcher.updateLimits();
                dailyNotifier.accept(day);
            } catch (IOException e) {
                LOG.warn("Error fetching daily stats", e);
            }
        }

        @Override
        public String toString() {
            return day;
        }
    }
}
