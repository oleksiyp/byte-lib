package news_service;

import news_api.ArticleJson;

import java.util.Date;
import java.util.Optional;

import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

public class News {
    private String author;
    private String description;
    private Date publishedAt;
    private String title;
    private String url;
    private String urlToImage;
    private float score;
    private Date fetchedAt;

    public News() {
    }

    public News(ArticleJson json) {
        author = ofNullable(json.getAuthor()).orElse("");
        description = ofNullable(json.getDescription()).orElse("");
        publishedAt = ofNullable(json.getPublishedAt()).orElse(new Date(0));
        title = ofNullable(json.getTitle()).orElse("");
        url = ofNullable(json.getUrl()).orElse("");
        urlToImage = ofNullable(json.getUrlToImage()).orElse("");
        score = 0;
        fetchedAt = new Date();
    }

    public News(String author, String description, Date publishedAt, String title, String url, String urlToImage, float score, Date fetchedAt) {
        this.author = author;
        this.description = description;
        this.publishedAt = publishedAt;
        this.title = title;
        this.url = url;
        this.urlToImage = urlToImage;
        this.score = score;
        this.fetchedAt = fetchedAt;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Date getPublishedAt() {
        return publishedAt;
    }

    public void setPublishedAt(Date publishedAt) {
        this.publishedAt = publishedAt;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUrlToImage() {
        return urlToImage;
    }

    public void setUrlToImage(String urlToImage) {
        this.urlToImage = urlToImage;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }

    public Date getFetchedAt() {
        return fetchedAt;
    }

    public void setFetchedAt(Date fetchedAt) {
        this.fetchedAt = fetchedAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        News news = (News) o;

        return url.equals(news.url);
    }

    @Override
    public int hashCode() {
        return url.hashCode();
    }
}
