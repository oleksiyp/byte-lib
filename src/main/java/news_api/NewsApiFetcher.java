package news_api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.squareup.okhttp.Cache;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.ResponseBody;
import news_service.News;
import news_service.NewsFetcher;
import news_service.NewsServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class NewsApiFetcher implements NewsFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(NewsApiFetcher.class);
    public static final String OK_STATUS = "ok";
    private final OkHttpClient client;
    private final ExecutorService executor;
    private final String apiKey;
    private String[] sortByOrder;

    public NewsApiFetcher(OkHttpClient client, ExecutorService executor, String apiKey, String[] sortByOrder) {
        this.client = client;
        this.executor = executor;
        this.apiKey = apiKey;
        this.sortByOrder = sortByOrder;
    }


    public Set<News> fetchNews() {
        Request request = new Request.Builder()
                .url("https://newsapi.org/v1/sources")
                .build();
        try (ResponseBody body = client.newCall(request).execute().body()) {
            SourcesJson sources = new ObjectMapper()
                    .readValue(body.byteStream(), SourcesJson.class);

            if (!OK_STATUS.equals(sources.getStatus())) {
                LOG.error(sources.getCode() + " " + sources.getMessage());
                return Collections.emptySet();
            }

            LOG.info("Fetching news from {} sources from NewsApi", sources.getSources().size());
            List<Future<List<ArticleJson>>> futures = executor.invokeAll(sources.getSources()
                    .stream()
                    .map(ArticlesFetchTask::new)
                    .collect(toList()));

            Set<News> news = futures.stream()
                    .flatMap(future -> {
                        try {
                            return future.get().stream();
                        } catch (Exception e) {
                            LOG.error("Failed to fetch", e);
                            return Stream.empty();
                        }
                    }).map(News::new)
                    .collect(Collectors.toSet());

            LOG.info("Fetched {} news", news.size());

            return news;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private class ArticlesFetchTask implements Callable<List<ArticleJson>> {
        private final Optional<String> sortBy;
        private String id;

        public ArticlesFetchTask(SourceJson json) {
            this.id = json.getId();
            sortBy = Stream.of(sortByOrder)
                    .filter(sortBy -> json.getSortBysAvailable().contains(sortBy))
                    .findFirst();
        }

        @Override
        public List<ArticleJson> call() throws Exception {
            if (!sortBy.isPresent()) {
                return Collections.emptyList();
            }

            String url = "https://newsapi.org/v1/articles" +
                    "?source=" + id +
                    "&sortBy=" + sortBy.get() +
                    "&apiKey=" + apiKey;
            Request request = new Request.Builder()
                    .url(url)
                    .build();

            try (ResponseBody body = client.newCall(request).execute().body()) {
                ArticlesJson json = new ObjectMapper().readValue(body.byteStream(),
                        ArticlesJson.class);

                if (!OK_STATUS.equals(json.getStatus())) {
                    LOG.error(json.getCode() + " " + json.getMessage());
                    return Collections.emptyList();
                }

                return json.getArticles();
            }
        }
    }
}
