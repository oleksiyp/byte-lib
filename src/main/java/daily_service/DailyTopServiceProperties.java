package daily_service;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.File;

@Component
@ConfigurationProperties(prefix = "daily_top_service")
public class DailyTopServiceProperties {
    private String gitRepo;
    private String cacheRepo;
    private String dumpsUrl;
    private File dumpsIndexHtmlCache;
    private String dumpsDownload;
    private File imagesData;
    private File depictionFile;
    private File thumbnailFile;
    private int dumpsIndexHtmlCacheSize;
    private File labelsData;
    private File labelsFile;
    private File interlinksData;
    private File mainPagesFile;
    private File templatePagesFile;
    private File specialPagesFile;
    private int topK;
    private String hourlyJsonDir;
    private String dailyJsonDir;
    private String limitsJsonFile;
    private int downloadParallelism;

    public String getGitRepo() {
        return gitRepo;
    }

    public void setGitRepo(String gitRepo) {
        this.gitRepo = gitRepo;
    }

    public String getCacheRepo() {
        return cacheRepo;
    }

    public void setCacheRepo(String cacheRepo) {
        this.cacheRepo = cacheRepo;
    }

    public String getDumpsUrl() {
        return dumpsUrl;
    }

    public void setDumpsUrl(String dumpsUrl) {
        this.dumpsUrl = dumpsUrl;
    }

    public File getDumpsIndexHtmlCache() {
        return dumpsIndexHtmlCache;
    }

    public void setDumpsIndexHtmlCache(File dumpsIndexHtmlCache) {
        this.dumpsIndexHtmlCache = dumpsIndexHtmlCache;
    }

    public String getDumpsDownload() {
        return dumpsDownload;
    }

    public void setDumpsDownload(String dumpsDownload) {
        this.dumpsDownload = dumpsDownload;
    }

    public File getImagesData() {
        return imagesData;
    }

    public void setImagesData(File imagesData) {
        this.imagesData = imagesData;
    }

    public File getDepictionFile() {
        return depictionFile;
    }

    public void setDepictionFile(File depictionFile) {
        this.depictionFile = depictionFile;
    }

    public File getThumbnailFile() {
        return thumbnailFile;
    }

    public void setThumbnailFile(File thumbnailFile) {
        this.thumbnailFile = thumbnailFile;
    }

    public int getDumpsIndexHtmlCacheSize() {
        return dumpsIndexHtmlCacheSize;
    }

    public void setDumpsIndexHtmlCacheSize(int dumpsIndexHtmlCacheSize) {
        this.dumpsIndexHtmlCacheSize = dumpsIndexHtmlCacheSize;
    }

    public File getLabelsData() {
        return labelsData;
    }

    public void setLabelsData(File labelsData) {
        this.labelsData = labelsData;
    }

    public File getLabelsFile() {
        return labelsFile;
    }

    public void setLabelsFile(File labelsFile) {
        this.labelsFile = labelsFile;
    }

    public File getInterlinksData() {
        return interlinksData;
    }

    public void setInterlinksData(File interlinksData) {
        this.interlinksData = interlinksData;
    }

    public File getMainPagesFile() {
        return mainPagesFile;
    }

    public void setMainPagesFile(File mainPagesFile) {
        this.mainPagesFile = mainPagesFile;
    }

    public File getTemplatePagesFile() {
        return templatePagesFile;
    }

    public void setTemplatePagesFile(File templatePagesFile) {
        this.templatePagesFile = templatePagesFile;
    }

    public File getSpecialPagesFile() {
        return specialPagesFile;
    }

    public void setSpecialPagesFile(File specialPagesFile) {
        this.specialPagesFile = specialPagesFile;
    }

    public int getTopK() {
        return topK;
    }

    public void setTopK(int topK) {
        this.topK = topK;
    }

    public String getHourlyJsonDir() {
        return hourlyJsonDir;
    }

    public void setHourlyJsonDir(String hourlyJsonDir) {
        this.hourlyJsonDir = hourlyJsonDir;
    }

    public String getDailyJsonDir() {
        return dailyJsonDir;
    }

    public void setDailyJsonDir(String dailyJsonDir) {
        this.dailyJsonDir = dailyJsonDir;
    }

    public String getLimitsJsonFile() {
        return limitsJsonFile;
    }

    public void setLimitsJsonFile(String limitsJsonFile) {
        this.limitsJsonFile = limitsJsonFile;
    }

    public int getDownloadParallelism() {
        return downloadParallelism;
    }

    public void setDownloadParallelism(int downloadParallelism) {
        this.downloadParallelism = downloadParallelism;
    }
}
