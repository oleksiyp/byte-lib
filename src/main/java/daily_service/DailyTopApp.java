package daily_service;

import com.squareup.okhttp.Cache;
import com.squareup.okhttp.OkHttpClient;
import dbpedia.DbpediaLookups;
import dbpedia.ImagesLookup;
import dbpedia.InterlinksLookup;
import dbpedia.LabelsLookup;
import download.WikimediaDataSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import website.Git;
import website.GitWebsiteUploader;
import website.WebsiteUploader;
import wikipageviews.PageViewFetcher;

import java.io.File;
import java.util.concurrent.ForkJoinPool;

@SpringBootApplication
public class DailyTopApp {
    @Autowired
    DailyTopServiceProperties properties;

    @Bean
    public WebsiteUploader uploader() {
        GitWebsiteUploader uploader = new GitWebsiteUploader(
                new Git(properties.getGitRepo()),
                new Git(properties.getCacheRepo()));

        uploader.setPathToDaily(new File(properties.getDailyJsonDir()));

        return uploader;
    }
    
    @Bean
    public PageViewFetcher fetcher(DbpediaLookups lookups) {
        OkHttpClient client = new OkHttpClient();

        return new PageViewFetcher(
                properties.getTopK(),
                properties.getHourlyJsonDir(),
                properties.getDailyJsonDir(),
                lookups,
                client,
                properties.getLimitsJsonFile());
    }

    @Bean
    DailyTopService dailyTopService(WikimediaDataSet dataSet,
                                    PageViewFetcher fetcher,
                                    WebsiteUploader uploader) {
        CustomizableThreadFactory threadFactory = new CustomizableThreadFactory("daily_top_service");

        PriorityExecutor perDayExecutor = new PriorityExecutor(properties.getDownloadParallelism(), threadFactory);

        return new DailyTopService(dataSet,
                fetcher,
                (day) -> uploader.update(),
                perDayExecutor);
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
                new ForkJoinPool(4));
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
    public DbpediaLookups lookups(ImagesLookup imagesLookup,
                                  LabelsLookup labelsLookup,
                                  InterlinksLookup interlinksLookup) {

        return new DbpediaLookups(imagesLookup,
                labelsLookup,
                interlinksLookup);
    }

    public static void main(String[] args) {
        SpringApplication.run(DailyTopApp.class, args);
    }
}
