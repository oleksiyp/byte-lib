package wikipageviews;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.squareup.okhttp.OkHttpClient;
import news_api.NewsApiFetcher;
import news_service.News;
import news_service.NewsFetcher;
import news_service.NewsService;
import news_service.NewsServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Math.E;
import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingInt;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static util.IOUtils.wrapIOException;

public class DailyCatTopAggregator {
    private static final Logger LOG = LoggerFactory.getLogger(DailyCatTopAggregator.class);

    private final int topK;
    private final String outDir;
    private final int topKCategories;
    private final int minRecordsPerCategory;
    private final int maxRecordsPerCategory;
    private final NewsService newsService;

    public DailyCatTopAggregator(int topK,
                                 String outDir,
                                 int topKCategories,
                                 int minRecordsPerCategory,
                                 int maxRecordsPerCategory,
                                 NewsService newsService) {
        this.topK = topK;
        this.outDir = outDir;
        this.topKCategories = topKCategories;

        this.minRecordsPerCategory = minRecordsPerCategory;
        this.maxRecordsPerCategory = maxRecordsPerCategory;
        this.newsService = newsService;
    }

    public void aggregate(String day, List<PageView> pageViews) {
        Aggregator aggregator = new Aggregator(pageViews);
        aggregator.run();

        for (PageViewRecordCategory category : aggregator.resultCats) {
            for (PageViewRecord record : category.getRecords()) {
                int position = record.getPosition();
                List<News> news = record.getNews();

                LOG.info("Writing news for '{}' on {}", record.getLabel(), day);
                File outFile = getNewsOutFile(day, position);
                outFile.getParentFile().mkdirs();
                wrapIOException(() -> new ObjectMapper().writeValue(outFile, news));
                record.setNewsCount(news.size());
                record.setNews(null);
            }
        }

        LOG.info("Writing top hourly records {}", getOutFile(day));
        File outFile = getOutFile(day);
        outFile.getParentFile().mkdirs();
        wrapIOException(() ->
            new ObjectMapper().writeValue(outFile, aggregator.resultCats));

    }

    private File getNewsOutFile(String day, int position) {
        return new File(outDir + "/" + day + "/" + position + ".json");
    }

    public File getOutFile(String day) {
        return new File(outDir + "/" + day + ".json");
    }

    public boolean hasOutFile(String day) {
        return getOutFile(day).isFile();
    }

    private class Aggregator implements Runnable {

        private final List<PageView> pageViews;

        private List<PageViewRecord> records;
        private List<PageViewRecordCategory> cats;

        private Set<PageViewRecord> categorizedRecords;
        private PageViewRecordCategory topCategory;

        private List<PageViewRecordCategory> resultCats;

        public Aggregator(List<PageView> pageViews) {
            this.pageViews = pageViews;
        }

        public void run(){
            mergeAndDeduplicate();
            groupCategories();
            newsScore();
            scoreAndSortCats();
            selectTopCategories();
            addRestCategory();
        }

        private void newsScore() {
            for (PageViewRecord record : records) {
                double newsScore =
                        record.getNews()
                                .stream()
                                .map(News::getScore)
                                .mapToDouble(Float::doubleValue)
                                .limit(2)
                                .sum() + E;

                record.setScore(record.getScore() * log(log(newsScore)));
            }

            for (PageViewRecordCategory category : cats) {
                double score = log(log(scoreByNews(category)) + E);
                category.setNewsScore(score);
            }

            records.sort(comparing(PageViewRecord::getScore).reversed());
            for (int i = 0; i < records.size(); i++) {
                records.get(i).setPosition(i);
            }
        }

        private void addRestCategory() {
            PageViewRecordCategory restCat = new PageViewRecordCategory();
            restCat.addAll(records.stream()
                    .filter(pvr -> !categorizedRecords.contains(pvr))
                    .sorted(comparing(PageViewRecord::getScore).reversed())
                    .limit(max(topK - categorizedRecords.size(), 0))
                    .collect(toList()));
            if (!restCat.getRecords().isEmpty()) {
                resultCats.add(restCat);
            }
        }

        private void selectTopCategories() {
            resultCats = new ArrayList<>();
            while (!cats.isEmpty()) {
                if (resultCats.size() >= topKCategories) {
                    break;
                }

                addSpareCategory();

                topCategory = cats.remove(0);
                topCategory.cutRecords(maxRecordsPerCategory, topK);
                if (topCategory.getRecords().size() < minRecordsPerCategory) {
                    scoreAndSortCats();
                    continue;
                }

                resultCats.add(topCategory);

                removeRecords(topCategory.getRecords());
                scoreAndSortCats();
            }

            categorizedRecords = resultCats.stream()
                    .flatMap((cat) -> cat.getRecords().stream())
                    .collect(Collectors.toSet());
        }

        private void addSpareCategory() {
            PageViewRecordCategory spareCategory = new PageViewRecordCategory();
            while (!records.isEmpty()) {

                PageViewRecord topRecord = records.get(0);
                PageViewRecordCategory topCat = cats.get(0);

                if (topCat.getRecords().isEmpty() ||
                        topRecord.getScore() - 1e-8 <= topCat.getRecords().get(0).getScore()) {
                    break;
                }

                removeRecords(singletonList(topRecord));

                scoreAndSortCats();
                spareCategory.getRecords().add(topRecord);
            }

            if (!spareCategory.getRecords().isEmpty()) {
                resultCats.add(spareCategory);

                List<PageViewRecord> records = spareCategory.getRecords();
                removeRecords(records);
            }
        }

        private void removeRecords(List<PageViewRecord> recordsToRemove) {
            Set<PageViewRecord> topRecords = new HashSet<>(recordsToRemove);
            cats.forEach(cat -> cat.getRecords().removeAll(topRecords));
            records.removeAll(topRecords);
        }

        private void scoreAndSortCats() {
            cats.forEach(PageViewRecordCategory::sortRecords);
            cats.forEach(PageViewRecordCategory::scoreRecords);
            cats.sort(comparingInt(PageViewRecordCategory::getScoreRounded).reversed());
        }

        private void groupCategories() {
            Map<String, PageViewRecordCategory> groupedPerCategory = new HashMap<>();

            records.forEach(pageViewRecord -> {
                pageViewRecord.getCategories()
                        .stream()
                        .distinct()
                        .forEach(category ->
                                groupedPerCategory.computeIfAbsent(category, PageViewRecordCategory::new)
                                        .add(pageViewRecord));
            });


            cats = new ArrayList<>(groupedPerCategory.values());
        }

        private void mergeAndDeduplicate() {
            records = new ArrayList<>(pageViews.stream()
                    .map(PageView::getTopRecords)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toMap(
                            PageViewRecord::getLangResource,
                            Function.identity(),
                            PageViewRecord::selectTopPageView))
                    .values());
        }

        private Double scoreByNews(PageViewRecordCategory cat) {
            return newsService.search(cat.getCategory(), 2, 0, null)
                    .stream()
                    .map(News::getScore)
                    .mapToDouble(Float::doubleValue)
                    .limit(2)
                    .sum() + E;
        }

    }


    public static void main(String[] args) {

        NewsFetcher fetcher = new NewsApiFetcher(new OkHttpClient(),
                Executors.newSingleThreadExecutor(),
                "",
                new String[0]);

        NewsServiceImpl newsService = new NewsServiceImpl(fetcher,
                new File("data/news/hourly"),
                new File("data/news/index"),
                false
                );

        DailyCatTopAggregator aggregator = new DailyCatTopAggregator(100,
                "parsed/daily_cat",
                10, 2, 12,
                newsService);

        List<PageView> pageViews = Stream.of(ofNullable(new File("parsed/hourly")
                .listFiles((dir, file) -> file.contains("20170519"))).orElse(new File[0]))
                .map(file ->
                        new PageView()
                                .setJsonOutDir("parsed/hourly")
                                .setFile(file.getAbsolutePath()))
                .peek(PageView::readHourlyJsonOut)
                .peek(pv -> pv.searchForNews(newsService, 10, 3))
                .collect(toList());

        aggregator.aggregate("20170519", pageViews);
    }
}
