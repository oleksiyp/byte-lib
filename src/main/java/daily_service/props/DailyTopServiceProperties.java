package daily_service.props;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "daily_top_service")
public class DailyTopServiceProperties {
    private List<GitWebsiteUploaderProperties> git;
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
    private Integer limitLastDays;
    private String fetcherCron;
    private File categoryLabelsData;
    private File categoryLabelsFile;
    private File articleCategoriesData;
    private File articleCategoriesFile;
    private String dailyCatJsonDir;
    private int topKCategories;
    private File blackList;
    private boolean closeAfterFetch;
    private int minRecordsPerCategory;
    private int maxRecordsPerCategory;

    public List<GitWebsiteUploaderProperties> getGit() {
        return git;
    }

    public void setGit(List<GitWebsiteUploaderProperties> git) {
        this.git = git;
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

    public Integer getLimitLastDays() {
        return limitLastDays;
    }

    public void setLimitLastDays(Integer limitLastDays) {
        this.limitLastDays = limitLastDays;
    }

    public String getFetcherCron() {
        return fetcherCron;
    }

    public void setFetcherCron(String fetcherCron) {
        this.fetcherCron = fetcherCron;
    }

    public File getCategoryLabelsData() {
        return categoryLabelsData;
    }

    public void setCategoryLabelsData(File categoryLabelsData) {
        this.categoryLabelsData = categoryLabelsData;
    }

    public File getCategoryLabelsFile() {
        return categoryLabelsFile;
    }

    public void setCategoryLabelsFile(File categoryLabelsFile) {
        this.categoryLabelsFile = categoryLabelsFile;
    }

    public File getArticleCategoriesData() {
        return articleCategoriesData;
    }

    public void setArticleCategoriesData(File articleCategoriesData) {
        this.articleCategoriesData = articleCategoriesData;
    }

    public File getArticleCategoriesFile() {
        return articleCategoriesFile;
    }

    public void setArticleCategoriesFile(File articleCategoriesFile) {
        this.articleCategoriesFile = articleCategoriesFile;
    }

    public String getDailyCatJsonDir() {
        return dailyCatJsonDir;
    }

    public void setDailyCatJsonDir(String dailyCatJsonDir) {
        this.dailyCatJsonDir = dailyCatJsonDir;
    }

    public int getTopKCategories() {
        return topKCategories;
    }

    public void setTopKCategories(int topKCategories) {
        this.topKCategories = topKCategories;
    }

    public File getBlackList() {
        return blackList;
    }

    public void setBlackList(File blackList) {
        this.blackList = blackList;
    }

    public boolean closeAfterFetch() {
        return closeAfterFetch;
    }

    public void setCloseAfterFetch(boolean closeAfterFetch) {
        this.closeAfterFetch = closeAfterFetch;
    }

    public int getMinRecordsPerCategory() {
        return minRecordsPerCategory;
    }

    public void setMinRecordsPerCategory(int minRecordsPerCategory) {
        this.minRecordsPerCategory = minRecordsPerCategory;
    }

    public int getMaxRecordsPerCategory() {
        return maxRecordsPerCategory;
    }

    public void setMaxRecordsPerCategory(int maxRecordsPerCategory) {
        this.maxRecordsPerCategory = maxRecordsPerCategory;
    }
}
