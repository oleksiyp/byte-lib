package wikipageviews;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.squareup.okhttp.OkHttpClient;
import dbpedia.DbpediaLookups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Math.max;
import static java.util.Comparator.comparing;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static wikipageviews.PageViewRecordCategory.OTHER_CATEGORY;

public class PageViewFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(PageViewFetcher.class);
    public static final Pattern DAILY_TOP_JSON_PATTERN = Pattern.compile("\\d{8}.json");

    private final OkHttpClient downloadClient;
    private final DbpediaLookups lookups;
    private final int topK;
    private final int topKCategories;
    private final String hourlyJsonDir;
    private final String dailyJsonDir;
    private final String dailyCatJsonDir;
    private final String limitsJsonFile;

    public PageViewFetcher(int topK,
                           int topKCategories,
                           String hourlyJsonDir,
                           String dailyJsonDir,
                           String dailyCatJsonDir,
                           DbpediaLookups lookups,
                           OkHttpClient downloadClient,
                           String limitsJsonFile) {
        this.topK = topK;
        this.topKCategories = topKCategories;
        this.hourlyJsonDir = hourlyJsonDir;
        this.lookups = lookups;
        this.downloadClient = downloadClient;
        this.dailyJsonDir = dailyJsonDir;
        this.dailyCatJsonDir = dailyCatJsonDir;
        this.limitsJsonFile = limitsJsonFile;
    }

    private void parseFile(PageView pageView) {
        try {
            pageView.setLookups(lookups)
                    .download(downloadClient)
                    .writeHourlyTopToJson(topK);
        } catch (IOException err) {
            throw new IOError(err);
        }
    }

    public void parseDay(String day, List<PageView> pageViews) throws IOException {
        pageViews.forEach(this::parseFile);

        aggregateDayTopWithCategories(pageViews, day);
        aggregateDayTop(pageViews, day);
    }

    private void aggregateDayTop(List<PageView> grouped, String day) throws IOException {
        List<PageViewRecord> topPerDay = grouped.stream()
                .map(PageView::getTopRecords)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(
                        PageViewRecord::getLangResource,
                        Function.identity(),
                        PageViewRecord::selectTopPageView))
                .values()
                .stream()
                .sorted(comparing(PageViewRecord::getScore).reversed())
                .limit(topK)
                .collect(toList());

        String fileName = dailyJsonDir + "/" + day + ".json";
        LOG.info("Writing top hourly records {}", fileName);
        new ObjectMapper().writeValue(new File(fileName), topPerDay);
    }

    private void aggregateDayTopWithCategories(List<PageView> grouped, String day) throws IOException {
        Collection<PageViewRecord> mergedAndDeduplicated = grouped.stream()
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
                .forEach(cat -> cat.scoreRecords(12));


        List<PageViewRecordCategory> allCategories = perCategory.values()
                .stream()
                .sorted(comparing(PageViewRecordCategory::getScore))
                .collect(toList());

        List<PageViewRecordCategory> categories = new ArrayList<>();
        while (categories.size() < topKCategories && !allCategories.isEmpty()) {
            PageViewRecordCategory topCategory = allCategories.remove(allCategories.size() - 1);
            categories.add(topCategory);

            HashSet<PageViewRecord> topRecords = new HashSet<>(topCategory.getRecords());
            allCategories.forEach(cat -> cat.getRecords().removeAll(topRecords));
            allCategories.forEach(cat -> cat.scoreRecords(12));
            allCategories.sort(comparing(PageViewRecordCategory::getScore));
        }

        categories.forEach(cat -> cat.cutRecords(12));

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

        String fileName = dailyCatJsonDir + "/" + day + ".json";
        LOG.info("Writing top hourly records {}", fileName);
        new ObjectMapper().writeValue(new File(fileName), categories);
    }

    public void updateLimits() {
        List<String> days =
                Stream.of(ofNullable(new File(dailyJsonDir).listFiles()).orElse(new File[0]))
                        .filter(file -> DAILY_TOP_JSON_PATTERN.matcher(file.getName()).matches())
                        .map(File::getName)
                        .map(str -> str.substring(0, 8))
                        .collect(toList());

        Optional<String> min = days.stream().min(Comparator.naturalOrder());
        Optional<String> max = days.stream().max(Comparator.naturalOrder());

        if (min.isPresent() && max.isPresent()) {
            DayLimits dayLimits = new DayLimits();
            dayLimits.setMin(min.get());
            dayLimits.setMax(max.get());
            LOG.info("Updating {}", dayLimits);
            try {
                new ObjectMapper().writeValue(new File(limitsJsonFile), dayLimits);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }
    }

    public void assignJsonOutDir(List<PageView> pageViews) {
        pageViews.forEach(pageView -> pageView.setJsonOutDir(hourlyJsonDir));
    }
}
