package news_service;

import java.util.Date;
import java.util.List;

public interface NewsService {
    List<News> search(String query, int limit, int daysSimilarity, Date date, boolean allPhrase);
}
