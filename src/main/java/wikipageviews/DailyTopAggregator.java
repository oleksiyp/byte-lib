package wikipageviews;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

public class DailyTopAggregator {
    private static final Logger LOG = LoggerFactory.getLogger(DailyTopAggregator.class);

    private final int topK;
    private final String outDir;

    public DailyTopAggregator(int topK, String outDir) {
        this.topK = topK;
        this.outDir = outDir;
    }

    public void aggregate(String day, List<PageView> pageViews) {
        List<PageViewRecord> topPerDay = pageViews.stream()
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

        String fileName = getOutFile(day);
        LOG.info("Writing top hourly records {}", fileName);
        try {
            File resultFile = new File(fileName);
            resultFile.getParentFile().mkdirs();
            new ObjectMapper().writeValue(resultFile, topPerDay);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getOutFile(String day) {
        return outDir + "/" + day + ".json";
    }

    public boolean hasOutFile(String day) {
        return new File(getOutFile(day)).isFile();
    }
}
