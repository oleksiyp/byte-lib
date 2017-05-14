package wikipageviews;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.Math.max;
import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.toList;
import static wikipageviews.PageViewRecordCategory.OTHER_CATEGORY;

public class DailyCatTopAggregator {
    private static final Logger LOG = LoggerFactory.getLogger(DailyCatTopAggregator.class);

    private final int topK;
    private final String outDir;
    private final int topKCategories;
    private final int minRecordsPerCategory;
    private final int maxRecordsPerCategory;

    public DailyCatTopAggregator(int topK,
                                 String outDir,
                                 int topKCategories,
                                 int minRecordsPerCategory,
                                 int maxRecordsPerCategory) {
        this.topK = topK;
        this.outDir = outDir;
        this.topKCategories = topKCategories;

        this.minRecordsPerCategory = minRecordsPerCategory;
        this.maxRecordsPerCategory = maxRecordsPerCategory;
    }

    public void aggregate(String day, List<PageView> pageViews) {
        List<PageViewRecord> mergedAndDeduplicated = pageViews.stream()
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

        for (int i = 0; i < mergedAndDeduplicated.size(); i++) {
            mergedAndDeduplicated.get(i).setPosition(i);
        }

        Map<String, PageViewRecordCategory> perCategory = new HashMap<>();

        mergedAndDeduplicated.forEach(pageViewRecord -> {
            pageViewRecord.getCategories()
                    .stream()
                    .distinct()
                    .forEach(category ->
                            perCategory.computeIfAbsent(category, PageViewRecordCategory::new)
                                    .add(pageViewRecord));
        });


        perCategory
                .values()
                .forEach(PageViewRecordCategory::scoreRecords);


        Comparator<PageViewRecordCategory> topCategories =
                comparingInt(PageViewRecordCategory::getScoreRounded)
                        .thenComparing(cat -> cat.getCategory().length());

        List<PageViewRecordCategory> allCategories = perCategory.values()
                .stream()
                .sorted(topCategories)
                .collect(toList());


        List<PageViewRecordCategory> categories = new ArrayList<>();
        while (categories.size() < topKCategories && !allCategories.isEmpty()) {
            PageViewRecordCategory topCategory = allCategories.remove(allCategories.size() - 1);
            topCategory.cutRecords(maxRecordsPerCategory, topK);
            if (topCategory.getRecords().size() < minRecordsPerCategory) {
                continue;
            }
            categories.add(topCategory);

            HashSet<PageViewRecord> topRecords = new HashSet<>(topCategory.getRecords());
            allCategories.forEach(cat -> cat.getRecords().removeAll(topRecords));
            allCategories.forEach(PageViewRecordCategory::scoreRecords);
            allCategories.sort(topCategories);
        }


        Set<PageViewRecord> categorizedRecords =
                categories.stream()
                        .flatMap((cat) -> cat.getRecords().stream())
                        .collect(Collectors.toSet());

        PageViewRecordCategory restCat = new PageViewRecordCategory(OTHER_CATEGORY);
        restCat.addAll(mergedAndDeduplicated.stream()
                .filter(pvr -> !categorizedRecords.contains(pvr))
                .sorted(comparing(PageViewRecord::getScore).reversed())
                .limit(max(topK - categorizedRecords.size(), 0))
                .collect(Collectors.toList()));
        if (!restCat.getRecords().isEmpty()) {
            categories.add(restCat);
        }

        String fileName = getOutFile(day);
        LOG.info("Writing top hourly records {}", fileName);
        try {
            File resultFile = new File(fileName);
            resultFile.getParentFile().mkdirs();
            new ObjectMapper().writeValue(resultFile, categories);
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
}
