package daily_service;

import com.squareup.okhttp.Cache;
import com.squareup.okhttp.OkHttpClient;
import daily_service.props.DailyTopServiceProperties;
import daily_service.props.GitWebsiteUploaderProperties;
import dbpedia.*;
import download.WikimediaDataSet;
import news_api.NewsApiFetcher;
import news_service.NewsFetcher;
import news_service.NewsService;
import news_service.NewsServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import util.PriorityExecutor;
import website.*;
import wikipageviews.BlackList;
import wikipageviews.DailyCatTopAggregator;
import wikipageviews.DailyTopAggregator;
import wikipageviews.PageViewFetcher;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

@SpringBootApplication
public class DailyTopApp {
    @Autowired
    DailyTopServiceProperties properties;

    @Bean
    public WebsiteUploader uploader() {
        List<WebsiteUploader> uploaders = new ArrayList<>();
        if (properties.getGit() != null) {
            uploaders.addAll(
                    properties.getGit()
                            .stream()
                            .map(this::createGitWebsiteUploader)
                            .collect(toList()));
        }

        WebsiteUploader uploader = new ManyWebsiteUploader(uploaders);

        uploader.setPathToDaily(new File(properties.getDailyJsonDir()));
        uploader.setPathToDailyCat(new File(properties.getDailyCatJsonDir()));

        return uploader;
    }

    private GitWebsiteUploader createGitWebsiteUploader(GitWebsiteUploaderProperties props) {
        GitWebsiteUploader uploader = new GitWebsiteUploader(
                new Git(props.getRepo()),
                new Git(new File(props.getCache())),
                props.getBranch(),
                props.getDailyDir());

        uploader.setCommitMessage(props.getCommitMessage());
        uploader.setUserEmail(props.getUserEmail());
        uploader.setUserName(props.getUserName());
        uploader.setGitRepoDailyCatPath(props.getDailyCatDir());

        return uploader;
    }

    @Bean
    public PageViewFetcher fetcher(DbpediaLookups lookups, NewsService newsService) {
        OkHttpClient client = new OkHttpClient();

        return new PageViewFetcher(
                properties.getTopK(),
                properties.getHourlyJsonDir(),
                lookups,
                client,
                new BlackList(properties.getBlackList())
        );
    }

    @Bean
    public NewsApiFetcher newsApiFetcher() {
        Cache cache = new Cache(
                properties.getNewsApiCache(),
                properties.getNewsApiCacheSize());

        OkHttpClient client = new OkHttpClient();
        client.setCache(cache);

        ExecutorService executor = Executors.newFixedThreadPool(
                properties.getNewsApiParallelism()
        );

        return new NewsApiFetcher(client,
                executor,
                properties.getNewsApiKey(), new String[]{"top", "popular", "latest"});
    }

    @Bean
    public NewsServiceImpl newsService(NewsFetcher fetcher) {
        return new NewsServiceImpl(fetcher,
                properties.getNewsServiceStore(),
                properties.getNewsServiceIndex(),
                properties.getNewsServiceReindex());
    }

    @Bean
    public DailyTopService dailyTopService(WikimediaDataSet dataSet,
                                           PageViewFetcher fetcher,
                                           WebsiteUploader uploader,
                                           NewsService newsService) {
        CustomizableThreadFactory threadFactory = new CustomizableThreadFactory("daily_top_service");
        threadFactory.setDaemon(true);

        PriorityExecutor perDayExecutor = new PriorityExecutor(properties.getDownloadParallelism(), threadFactory);

        LimitsJsonUpdater dailyLimitsJsonUpdater = new LimitsJsonUpdater(
                new File(properties.getDailyJsonDir()),
                new File(properties.getDailyJsonDir(), properties.getLimitsJsonFile())
        );

        LimitsJsonUpdater dailyCatLimitsJsonUpdater = new LimitsJsonUpdater(
                new File(properties.getDailyCatJsonDir()),
                new File(properties.getDailyCatJsonDir(), properties.getLimitsJsonFile())
        );

        DailyTopAggregator aggregator = new DailyTopAggregator(
                properties.getTopK(),
                properties.getDailyJsonDir());

        DailyCatTopAggregator catAggregator = new DailyCatTopAggregator(
                properties.getTopK(),
                properties.getDailyCatJsonDir(),
                properties.getTopKCategories(),
                properties.getMinRecordsPerCategory(),
                properties.getMaxRecordsPerCategory(),
                newsService);

        return new DailyTopService(dataSet,
                fetcher,
                (day) -> {
                    synchronized (uploader) {
                        dailyLimitsJsonUpdater.updateLimitsJson();
                        dailyCatLimitsJsonUpdater.updateLimitsJson();
                        uploader.update();
                    }
                },
                perDayExecutor,
                ofNullable(properties.getLimitLastDays()),
                aggregator,
                catAggregator,
                properties.isCloseAfterFetch(),
                newsService,
                properties.getNewsLimit(),
                properties.getNewsDaysSimiliarity());
    }

    @Bean
    public WikimediaDataSet wikimediaDumps() {
        Cache cache = new Cache(
                properties.getDumpsIndexHtmlCache(),
                properties.getDumpsIndexHtmlCacheSize());

        OkHttpClient client = new OkHttpClient();
        client.setCache(cache);

        return new WikimediaDataSet(
                properties.getDumpsUrl(),
                client,
                properties.getDumpsDownload(),
                new ForkJoinPool(1));
    }

    @Bean
    public ImagesLookup imagesLookup() {
        return new ImagesLookup(
                properties.getImagesData(),
                properties.getDepictionFile(),
                properties.getThumbnailFile());
    }

    @Bean
    public LabelsLookup labelsLookup() {
        return new LabelsLookup(
                properties.getLabelsData(),
                properties.getLabelsFile()
        );
    }

    @Bean
    public InterlinksLookup interlinksLookup() {
        return new InterlinksLookup(
                properties.getInterlinksData(),
                properties.getMainPagesFile(),
                properties.getTemplatePagesFile(),
                properties.getSpecialPagesFile()
        );
    }

    @Bean
    public CategoryLabelsLookup categoryLabelsLookup() {
        return new CategoryLabelsLookup(
                properties.getCategoryLabelsData(),
                properties.getCategoryLabelsFile()
        );
    }

    @Bean
    public ArticleCategoryLookup articleCategoryLookup(CategoryLabelsLookup categoryLabelsLookup) {
        return new ArticleCategoryLookup(
                properties.getArticleCategoriesData(),
                properties.getArticleCategoriesFile(),
                categoryLabelsLookup
        );
    }

    @Bean
    public DbpediaLookups lookups(ImagesLookup imagesLookup,
                                  LabelsLookup labelsLookup,
                                  InterlinksLookup interlinksLookup,
                                  ArticleCategoryLookup articleCategoryLookup) {

        return new DbpediaLookups(imagesLookup,
                labelsLookup,
                interlinksLookup,
                articleCategoryLookup);
    }


    public static void main(String[] args) {
        ConfigurableApplicationContext ctx = SpringApplication.run(DailyTopApp.class, args);
        DailyTopServiceProperties properties = ctx.getBean(DailyTopServiceProperties.class);
        if (properties.isCloseAfterFetch()) {
            ctx.close();
        }
    }
}
