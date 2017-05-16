package daily_service;

import download.WikimediaDataSet;
import news_service.NewsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.PriorityExecutor;
import wikipageviews.DailyCatTopAggregator;
import wikipageviews.DailyTopAggregator;
import wikipageviews.PageView;
import wikipageviews.PageViewFetcher;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Optional.*;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class DailyTopService {
    private final static Logger LOG = LoggerFactory.getLogger(DailyTopService.class);

    private final WikimediaDataSet dataSet;
    private PageViewFetcher fetcher;
    private Consumer<String> dailyNotifier;
    private PriorityExecutor perDayExecutor;
    private Optional<Integer> limitLastDays;
    private final DailyTopAggregator aggregator;
    private final DailyCatTopAggregator catAggregator;
    private boolean waitFetchOnInit;
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
    private final NewsService newsService;
    private final int newsLimit;
    private final int newsDaysSimilarity;

    public DailyTopService(WikimediaDataSet dataSet,
                           PageViewFetcher fetcher,
                           Consumer<String> dailyNotifier,
                           PriorityExecutor perDayExecutor,
                           Optional<Integer> limitLastDays,
                           DailyTopAggregator aggregator,
                           DailyCatTopAggregator catAggregator,
                           boolean waitFetchOnInit,
                           NewsService newsService,
                           int newsLimit,
                           int newsDaysSimilarity) {
        this.dataSet = dataSet;
        this.fetcher = fetcher;
        this.dailyNotifier = dailyNotifier;

        this.perDayExecutor = perDayExecutor;
        this.limitLastDays = limitLastDays;

        this.aggregator = aggregator;
        this.catAggregator = catAggregator;
        this.waitFetchOnInit = waitFetchOnInit;
        this.newsService = newsService;

        this.newsLimit = newsLimit;
        this.newsDaysSimilarity = newsDaysSimilarity;
    }

    @PostConstruct
    public void init() {
        CompletableFuture<Void> task = run();
        if (waitFetchOnInit) {
            try {
                task.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public CompletableFuture<Void> run() {
        List<PageView> fetchedPageViews = dataSet
                .fetchPageviews()
                .getPageViews();

        fetcher.assignJsonOutDir(fetchedPageViews);

        Map<String, List<PageView>> perDay = fetchedPageViews
                .stream()
                .collect(groupingBy(PageView::getDay));

        List<PageViewDailyFetchTask> tasks = daysHasMissingJsons(fetchedPageViews)
                .stream()
                .map(day -> new PageViewDailyFetchTask(day, perDay.get(day)))
                .collect(toList());

        return perDayExecutor.executeAll(tasks);
    }

    private List<String> daysHasMissingJsons(List<PageView> fetchedPageViews) {
        Optional<String> dayAfter = fetchedPageViews.stream()
                .map(PageView::getDay)
                .max(String::compareTo)
                .flatMap(this::subtractLimitDays);

        return fetchedPageViews
                .stream()
                .filter(pageView -> compareDays(pageView.getDay(), dayAfter))
                .filter(this::hasJsonMissing)
                .map(PageView::getDay)
                .collect(toSet())
                .stream()
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());
    }

    private boolean hasJsonMissing(PageView pageView) {
        return !pageView.hasJsonOut() ||
                !aggregator.hasOutFile(pageView.getDay()) ||
                !catAggregator.hasOutFile(pageView.getDay());
    }

    private Optional<String> subtractLimitDays(String lastDay) {
        if (!limitLastDays.isPresent()) {
            return empty();
        }

        try {
            Date lastDate = DATE_FORMAT.parse(lastDay);
            Calendar lastDateCal = Calendar.getInstance();
            lastDateCal.setTime(lastDate);
            lastDateCal.add(Calendar.DATE, -limitLastDays.get());
            return of(DATE_FORMAT.format(lastDateCal.getTime()));
        } catch (ParseException e) {
            return empty();
        }
    }

    private boolean compareDays(String day, Optional<String> from) {
        return from
                .map(fromVal -> fromVal.compareTo(day) <= 0)
                .orElse(true);

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
                fetcher.processDay(day, pageViews);
                pageViews.forEach(pageView ->
                        pageView.searchForNews(newsService, newsLimit, newsDaysSimilarity));
                aggregator.aggregate(day, pageViews);
                catAggregator.aggregate(day, pageViews);
                dailyNotifier.accept(day);
            } catch (IOException e) {
                LOG.warn("Error fetching daily stats", e);
            }
        }

        @Override
        public String toString() {
            return day;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PageViewDailyFetchTask that = (PageViewDailyFetchTask) o;

            return day.equals(that.day);
        }

        @Override
        public int hashCode() {
            return day.hashCode();
        }
    }
}
