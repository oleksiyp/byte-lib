package download;

import byte_lib.Progress;
import com.squareup.okhttp.Cache;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import wikipageviews.PageView;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;

import static java.util.Collections.emptySet;
import static java.util.Collections.reverseOrder;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toList;

public class DumpWikimediaPageViews {
    public static final String START_URL = "https://dumps.wikimedia.org/other/pageviews/";

    public static final String BASE_DIR = "data/dump.wikimedia.org";

    private final OkHttpClient client;
    private final String downloadBaseDir;
    private String startUrl;
    private List<PageView> pageViews;

    public static final Pattern PAGE_VIEW_PATTERN = Pattern.compile("\\d+/\\d+-\\d+/pageviews-\\d+-\\d+\\.gz");

    public DumpWikimediaPageViews(String startUrl, File cacheFile, String downloadBaseDir) {
        this.startUrl = startUrl;

        int cacheSize = 200 * 1024 * 1024; // 10 MiB
        Cache cache = new Cache(cacheFile, cacheSize);

        client = new OkHttpClient();
        client.setCache(cache);

        this.downloadBaseDir = downloadBaseDir;
    }

    public DumpWikimediaPageViews init() {
        ForkJoinPool pool = new ForkJoinPool(4);
        Set<String> allUrls = pool.invoke(new DownloadIndexTask(client, startUrl));

        pageViews = allUrls.stream()
                .filter((url) -> url.startsWith(startUrl))
                .filter((url) -> isPageViewRelativeUrl(relativeUrl(url)))
                .sorted(reverseOrder())
                .map((url) -> new PageView()
                        .setUrl(url)
                        .setFile(downloadBaseDir + relativeUrl(url)))
                .collect(toList());

        return this;
    }

    private boolean isPageViewRelativeUrl(String input) {
        return PAGE_VIEW_PATTERN
                .matcher(input)
                .matches();
    }

    private String relativeUrl(String url) {
        return url.substring(startUrl.length());
    }

    public List<PageView> getPageViews() {
        return pageViews;
    }

    public static DumpWikimediaPageViews fromMarch2015() {
        return fromMarch2015(BASE_DIR);
    }

    public static DumpWikimediaPageViews fromMarch2015(String baseDir) {
        return new DumpWikimediaPageViews(
                START_URL,
                new File(baseDir + "/cache"),
                baseDir + "/download/")
                .init();
    }

    private static class DownloadIndexTask extends RecursiveTask<Set<String>> {
        private OkHttpClient client;
        private String url;
        public List<String> list;

        public DownloadIndexTask(OkHttpClient client, String url) {
            this.client = client;
            this.url = url;
        }

        @Override
        protected Set<String> compute() {
            Response response;
            try {
                response = httpGet();
            } catch (IOException e) {
                return emptySet();
            }

            Set<String> list = new HashSet<>();
            extractLinks(response, list);

            List<DownloadIndexTask> subTasks = new ArrayList<>();
            for (String referenced : list) {
                DownloadIndexTask task = indexDownloadTask(referenced);
                if (task == null) {
                    continue;
                }
                task.fork();
                subTasks.add(task);
            }

            for (DownloadIndexTask task : subTasks) {
                list.addAll(task.join());
            }

            return list;
        }

        private DownloadIndexTask indexDownloadTask(String referencedUrl) {
            if (!referencedUrl.startsWith(url)) {
                return null;
            }

            if (referencedUrl.contains("projectviews")
                    || referencedUrl.endsWith(".gz")) {
                return null;
            }

            return new DownloadIndexTask(client, referencedUrl);
        }

        private void extractLinks(Response response, Set<String> list) {
            try {
                Document doc = Jsoup.parse(response.body().string(), url);
                doc.select("a").forEach((el) -> {
                    String url = el.attr("abs:href");
                    list.add(url);
                });
            } catch (IOException e) {
                // skip
            }
        }

        private Response httpGet() throws IOException {
            Response response;Request url = new Request.Builder()
                    .url(this.url)
                    .build();
            response = client.newCall(url).execute();
            return response;
        }
    }


}
