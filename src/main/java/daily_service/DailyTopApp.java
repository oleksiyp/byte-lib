package daily_service;

import com.squareup.okhttp.Cache;
import com.squareup.okhttp.OkHttpClient;
import daily_service.props.DailyTopServiceProperties;
import daily_service.props.GitWebsiteUploaderProperties;
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
import util.PriorityExecutor;
import website.Git;
import website.GitWebsiteUploader;
import website.ManyWebsiteUploader;
import website.WebsiteUploader;
import wikipageviews.BlackList;
import wikipageviews.PageViewFetcher;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
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
                properties.getLimitsJsonFile(),
                new BlackList(properties.getBlackList()));
    }

    @Bean
    public DailyTopService dailyTopService(WikimediaDataSet dataSet,
                                           PageViewFetcher fetcher,
                                           WebsiteUploader uploader) {
        CustomizableThreadFactory threadFactory = new CustomizableThreadFactory("daily_top_service");

        PriorityExecutor perDayExecutor = new PriorityExecutor(properties.getDownloadParallelism(), threadFactory);


        return new DailyTopService(dataSet,
                fetcher,
                (day) -> {
                    synchronized (uploader) {
                        uploader.update();
                    }
                },
                perDayExecutor,
                ofNullable(properties.getLimitLastDays()));
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
