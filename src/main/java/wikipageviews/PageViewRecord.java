package wikipageviews;

import news_service.News;

import java.util.List;

public class PageViewRecord {
    private String lang;
    private String resource;
    private int statCounter;
    private double score;
    private String thumbnail;
    private String depiction;
    private String label;
    private List<String> categories;
    private int position;
    private List<News> news;
    private int newsCount;

    public PageViewRecord() {
    }

    public PageViewRecord(String lang,
                          String resource,
                          int statCounter,
                          String thumbnail,
                          String depiction,
                          String label,
                          double score,
                          List<String> categories) {
        this.lang = lang;
        this.resource = resource;
        this.statCounter = statCounter;
        this.thumbnail = thumbnail;
        this.depiction = depiction;
        this.label = label;
        this.score = score;
        this.categories = categories;
    }

    public static String getLangResource(PageViewRecord pvr) {
        return pvr.getLang() + " " + pvr.getResource();
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public int getStatCounter() {
        return statCounter;
    }

    public void setStatCounter(int statCounter) {
        this.statCounter = statCounter;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public String getThumbnail() {
        return thumbnail;
    }

    public void setThumbnail(String thumbnail) {
        this.thumbnail = thumbnail;
    }

    public String getDepiction() {
        return depiction;
    }

    public void setDepiction(String depiction) {
        this.depiction = depiction;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public List<String> getCategories() {
        return categories;
    }

    public void setCategories(List<String> categories) {
        this.categories = categories;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public int getPosition() {
        return position;
    }

    public static PageViewRecord selectTopPageView(PageViewRecord pageViewRecord1,
                                                   PageViewRecord pageViewRecord2) {
        if (pageViewRecord1.score > pageViewRecord2.score) {
            return pageViewRecord1;
        } else {
            return pageViewRecord2;
        }
    }

    public void setNews(List<News> news) {
        this.news = news;
    }

    public List<News> getNews() {
        return news;
    }


    public int getNewsCount() {
        return newsCount;
    }

    public void setNewsCount(int newsCount) {
        this.newsCount = newsCount;
    }

}
