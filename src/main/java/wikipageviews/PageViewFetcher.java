package wikipageviews;

import com.squareup.okhttp.OkHttpClient;
import dbpedia.DbpediaLookups;
import news_service.NewsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static java.lang.Math.max;
import static java.util.Comparator.comparing;

public class PageViewFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(PageViewFetcher.class);

    private final OkHttpClient downloadClient;
    private final DbpediaLookups lookups;
    private final int topK;
    private final String hourlyJsonDir;
    private final BlackList blackList;

    public PageViewFetcher(int topK,
                           String hourlyJsonDir,
                           DbpediaLookups lookups,
                           OkHttpClient downloadClient,
                           BlackList blackList) {
        this.topK = topK;
        this.hourlyJsonDir = hourlyJsonDir;
        this.lookups = lookups;
        this.downloadClient = downloadClient;
        this.blackList = blackList;
    }

    public void processDay(String day, List<PageView> pageViews) throws IOException {
        LOG.info("Processing day {}, {} files", day, pageViews.size());

        pageViews.forEach(pageView ->
                pageView.setLookups(lookups)
                        .setBlackList(blackList)
                        .download(downloadClient)
                        .readOrParse(topK)
                        .write()
                        .removeDownloaded());
    }


    public void assignJsonOutDir(List<PageView> pageViews) {
        pageViews.forEach(pageView -> pageView.setJsonOutDir(hourlyJsonDir));
    }
}
