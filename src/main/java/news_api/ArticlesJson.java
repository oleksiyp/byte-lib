package news_api;

import java.util.List;

public class ArticlesJson {
    private String status;
    private String source;
    private String sortBy;
    private String code;
    private String message;
    private List<ArticleJson> articles;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getSortBy() {
        return sortBy;
    }

    public void setSortBy(String sortBy) {
        this.sortBy = sortBy;
    }
    
    public List<ArticleJson> getArticles() {
        return articles;
    }

    public void setArticles(List<ArticleJson> articles) {
        this.articles = articles;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
