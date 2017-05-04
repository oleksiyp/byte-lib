package wikipageviews;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.squareup.okhttp.OkHttpClient;
import dbpedia.DbpediaLookups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static byte_lib.io.ByteFiles.printStream;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

public class PageViewFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(PageViewFetcher.class);
    public static final Pattern DAILY_TOP_JSON_PATTERN = Pattern.compile("\\d{6}.json");

    private final OkHttpClient downloadClient;
    private final DbpediaLookups lookups;
    private final int topK;
    private final String hourlyJsonDir;
    private final String dailyJsonDir;
    private final String limitsJsonFile;

    public PageViewFetcher(int topK,
                           String hourlyJsonDir,
                           String dailyJsonDir,
                           DbpediaLookups lookups,
                           OkHttpClient downloadClient,
                           String limitsJsonFile) {
        this.topK = topK;
        this.hourlyJsonDir = hourlyJsonDir;
        this.lookups = lookups;
        this.downloadClient = downloadClient;
        this.dailyJsonDir = dailyJsonDir;
        this.limitsJsonFile = limitsJsonFile;
    }

    private void parseFile(PageView pageView) {
        try {
            pageView.setLookups(lookups)
                    .setJsonOutDir(hourlyJsonDir)
                    .download(downloadClient)
                    .writeHourlyTopToJson(topK);
        } catch (IOException err) {
            throw new IOError(err);
        }
    }

    public void parseDay(String day, List<PageView> pageViews) throws IOException {
        pageViews.forEach(this::parseFile);

        aggregateDayTop(pageViews, day);
    }

    private void aggregateDayTop(List<PageView> grouped, String day) throws IOException {
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

        String fileName = dailyJsonDir + "/" + day + ".json";
        LOG.info("Writing top hourly records {}", fileName);
        new ObjectMapper().writeValue(new File(fileName), topPerDay);
    }

    public void updateLimits() {
        List<String> days =
                Stream.of(ofNullable(new File(dailyJsonDir).listFiles()).orElse(new File[0]))
                        .filter(file -> DAILY_TOP_JSON_PATTERN.matcher(file.getName()).matches())
                        .map(File::getName)
                        .map(str -> str.substring(0, 6))
                        .collect(toList());

        Optional<String> min = days.stream().min(Comparator.naturalOrder());
        Optional<String> max = days.stream().max(Comparator.naturalOrder());

        if (min.isPresent() && max.isPresent()) {
            String fileName = limitsJsonFile;
            DayLimits dayLimits = new DayLimits();
            dayLimits.setMin(min.get());
            dayLimits.setMax(max.get());
            LOG.info("Updating {}", dayLimits);
            try {
                new ObjectMapper().writeValue(new File(fileName), dayLimits);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }
    }
}
