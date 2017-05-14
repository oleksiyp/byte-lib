package website;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wikipageviews.DayLimits;
import wikipageviews.PageViewFetcher;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

public class LimitsJsonUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(LimitsJsonUpdater.class);

    public static final Pattern DAILY_TOP_JSON_PATTERN = Pattern.compile("\\d{8}.json");

    private final File dailyJsonDir;
    private final File limitsJsonFile;

    public LimitsJsonUpdater(File dailyJsonDir, File limitsJsonFile) {
        this.dailyJsonDir = dailyJsonDir;
        this.limitsJsonFile = limitsJsonFile;
    }

    public void updateLimitsJson() {
        List<String> days =
                Stream.of(ofNullable(dailyJsonDir.listFiles()).orElse(new File[0]))
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
                new ObjectMapper().writeValue(limitsJsonFile, dayLimits);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }
    }
}
