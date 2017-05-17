package wikipageviews;

import com.fasterxml.jackson.databind.ObjectMapper;
import news_service.News;
import news_service.NewsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.Math.max;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingInt;
import static java.util.Optional.ofNullable;
import static wikipageviews.PageViewRecordCategory.OTHER_CATEGORY;

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

        String fileName = getOutFile(day);
        LOG.info("Writing top hourly records {}", fileName);
        try {
            File resultFile = new File(fileName);
            resultFile.getParentFile().mkdirs();
            new ObjectMapper().writeValue(resultFile, aggregator.resultCats);
        } catch (IOException ex) {
            throw new IOError(ex);
        }

    }

    public String getOutFile(String day) {
        return outDir + "/" + day + ".json";
    }

    public boolean hasOutFile(String day) {
        return new File(getOutFile(day)).isFile();
    }

    private class Aggregator implements Runnable {
        public final Comparator<PageViewRecordCategory> TOP_CATEGORIES_COMPARATOR = comparingInt(PageViewRecordCategory::getScoreRounded).reversed();

        private final List<PageView> pageViews;

        private List<PageViewRecord> records;
        private List<PageViewRecordCategory> cats;

        private Set<PageViewRecord> categorizedRecords;
        private List<PageViewRecordCategory> sameScoreCategories;
        private PageViewRecordCategory topCategory;

        private List<PageViewRecordCategory> resultCats;

        public Aggregator(List<PageView> pageViews) {
            this.pageViews = pageViews;
        }

        public void run(){
            mergeAndDeduplicate();
            groupCategories();
            scoreAndSortCats();
            selectTopCategories();
            addRestCategory();
        }

        private void addRestCategory() {
            PageViewRecordCategory restCat = new PageViewRecordCategory(OTHER_CATEGORY);
            restCat.addAll(records.stream()
                    .filter(pvr -> !categorizedRecords.contains(pvr))
                    .sorted(comparing(PageViewRecord::getScore).reversed())
                    .limit(max(topK - categorizedRecords.size(), 0))
                    .collect(Collectors.toList()));
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

                removeSameScoredTopCategories();
                selectTopByNews();

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

        private void removeSameScoredTopCategories() {
            PageViewRecordCategory topCategory = cats.remove(0);
            List<PageViewRecordCategory> sameScoreCategory = new ArrayList<>();
            while (!cats.isEmpty() &&
                    cats.get(0).getScoreRounded() == topCategory.getScoreRounded()) {
                sameScoreCategory.add(cats.remove(0));
            }
            sameScoreCategory.add(topCategory);
            sameScoreCategories = sameScoreCategory;
        }

        private void addSpareCategory() {
            PageViewRecordCategory spareCategory = new PageViewRecordCategory("");
            while (!records.isEmpty()) {

                PageViewRecord topRecord = records.get(0);
                PageViewRecordCategory topCat = cats.get(0);

                if (topRecord.getScore() < topCat.getScore()) {
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

        private void removeRecords(List<PageViewRecord> records) {
            Set<PageViewRecord> topRecords = new HashSet<>(records);
            cats.forEach(cat -> cat.getRecords().removeAll(topRecords));
            records.removeAll(topRecords);
        }

        private void scoreAndSortCats() {
            cats.forEach(PageViewRecordCategory::scoreRecords);
            cats.sort(TOP_CATEGORIES_COMPARATOR);
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
            records = pageViews.stream()
                    .map(PageView::getTopRecords)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toMap(
                            PageViewRecord::getLangResource,
                            Function.identity(),
                            PageViewRecord::selectTopPageView))
                    .values()
                    .stream()
                    .sorted(comparing(PageViewRecord::getScore).reversed())
                    .collect(Collectors.toList());

            for (int i = 0; i < records.size(); i++) {
                records.get(i).setPosition(i);
            }
        }

        private void selectTopByNews() {
            sameScoreCategories.sort(comparing(this::searchCategory).reversed());
            topCategory = sameScoreCategories.get(0);
        }

        private Double searchCategory(PageViewRecordCategory cat) {
            return newsService.search(cat.getCategory(), 1, 0, null)
                    .stream()
                    .map(News::getScore)
                    .findFirst()
                    .orElse(0.0f)
                    .doubleValue();
        }
    }
}
