package news_service;

import java.util.Set;

public interface NewsFetcher {
    Set<News> fetchNews();
}
