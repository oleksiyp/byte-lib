package download;

import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wikipageviews.PageView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;

import static java.util.Collections.emptySet;
import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toList;

public class WikimediaDataSet {
    private static final Logger LOG = LoggerFactory.getLogger(WikimediaDataSet.class);

    public static final Pattern PAGE_VIEW_PATTERN = Pattern.compile("\\d+/\\d+-\\d+/pageviews-\\d+-\\d+\\.gz");

    private final OkHttpClient client;
    private final String downloadBaseDir;
    private final String startUrl;
    private List<PageView> pageViews;
    private final ForkJoinPool pool;

    public WikimediaDataSet(String startUrl,
                            OkHttpClient indexHtmlClient,
                            String downloadBaseDir,
                            ForkJoinPool pool) {
        this.startUrl = startUrl;
        this.downloadBaseDir = downloadBaseDir;
        this.client = indexHtmlClient;
        this.pool = pool;
    }

    public WikimediaDataSet fetchPageviews() {
        LOG.info("Fetching indexes from {}", startUrl);
        Set<String> allUrls = pool.invoke(new DownloadIndexHtmlTask(client, startUrl));

        pageViews = allUrls.stream()
                .filter((url) -> url.startsWith(startUrl))
                .filter((url) -> isPageViewRelativeUrl(relativeUrl(url)))
                .sorted(reverseOrder())
                .map((url) -> new PageView()
                        .setUrl(url)
                        .setFile(downloadBaseDir + relativeUrl(url)))
                .collect(toList());
        LOG.info("Fetched {} pageviews", pageViews.size());
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

    private static class DownloadIndexHtmlTask extends RecursiveTask<Set<String>> {
        private OkHttpClient client;
        private String url;
        public List<String> list;

        public DownloadIndexHtmlTask(OkHttpClient client, String url) {
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

            List<DownloadIndexHtmlTask> subTasks = new ArrayList<>();
            for (String referenced : list) {
                DownloadIndexHtmlTask task = indexDownloadTask(referenced);
                if (task == null) {
                    continue;
                }
                task.fork();
                subTasks.add(task);
            }

            for (DownloadIndexHtmlTask task : subTasks) {
                list.addAll(task.join());
            }

            return list;
        }

        private DownloadIndexHtmlTask indexDownloadTask(String referencedUrl) {
            if (!referencedUrl.startsWith(url)) {
                return null;
            }

            if (referencedUrl.contains("projectviews")
                    || referencedUrl.endsWith(".gz")) {
                return null;
            }

            return new DownloadIndexHtmlTask(client, referencedUrl);
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
