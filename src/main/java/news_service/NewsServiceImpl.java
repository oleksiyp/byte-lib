package news_service;

import byte_lib.io.ByteFiles;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.of;
import static org.apache.lucene.search.BooleanClause.Occur.*;

public class NewsServiceImpl implements NewsService, Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(NewsServiceImpl.class);

    private final NewsFetcher fetcher;
    private final File newsStoreDir;
    private boolean reindexAtStart;
    private final Directory newsIndexDir;
    private final Analyzer analyzer;
    private final StandardQueryParser queryParser;

    public NewsServiceImpl(NewsFetcher fetcher, File newsStoreDir, File newsIndexDir, boolean reindexAtStart) {
        this.fetcher = fetcher;
        this.newsStoreDir = newsStoreDir;
        this.reindexAtStart = reindexAtStart;
        try {
            this.newsIndexDir = new SimpleFSDirectory(newsIndexDir.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        analyzer = new EnglishAnalyzer();
        queryParser = new StandardQueryParser(analyzer);
    }

    @PostConstruct
    public void init() {
        if (reindexAtStart) {
            reindex();
        }
        run();
    }

    public void reindex() {
        File[] jsons = ofNullable(newsStoreDir.listFiles(
                (File dir, String file) ->
                        file.matches("news-.+\\.json.*")))
                .orElse(new File[0]);

        LOG.info("Reindexing {} files", jsons.length);
        Set<News> news = of(jsons)
                .flatMap(file -> {
                    try (InputStream in = ByteFiles.inputStream(file)) {
                        Object newsSet = new ObjectMapper().readValue(in, new TypeReference<Set<News>>() {
                        });
                        return ((Set<News>) newsSet).stream();
                    } catch (IOException e) {
                        throw new IOError(e);
                    }
                }).collect(toSet());

        LOG.info("Reindexing {} news", news.size());

        try {
            for (String name : newsIndexDir.listAll()) {
                try {
                    newsIndexDir.deleteFile(name);
                } catch (IOException e) {
                    // skip
                }
            }
        } catch (IOException e) {
            throw new IOError(e);
        }

        index(news);
    }

    public void run() {
        Set<News> news = fetcher.fetchNews();
        writeJson(news);
        index(news);
    }

    private void index(Set<News> news) {
        try (IndexWriter writer = new IndexWriter(newsIndexDir, new IndexWriterConfig(analyzer))) {
            news.stream()
                    .map(this::toDocument)
                    .forEach(doc -> {
                        Term term = new Term("url", doc.get("url"));
                        try {
                            writer.updateDocument(term, doc);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private void writeJson(Set<News> news) {
        String now = new SimpleDateFormat("yyyyMMdd'-'HHmmss").format(new Date());
        newsStoreDir.mkdirs();
        File outFile = new File(newsStoreDir, "news-" + now + ".json.snappy");
        try (PrintStream out = ByteFiles.printStream(outFile)) {
            new ObjectMapper().writeValue(out, news);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<News> search(String queryStr, int limit, int daysSimilarity, Date date) {
        try (IndexReader reader = DirectoryReader.open(newsIndexDir)) {
            IndexSearcher searcher = new IndexSearcher(reader);

            BooleanQuery.Builder builder = new BooleanQuery.Builder();

            if (date != null) {
                long lowBound = date.getTime() - TimeUnit.DAYS.toMillis(daysSimilarity);
                long highBound = date.getTime() + TimeUnit.DAYS.toMillis(daysSimilarity);
                Query dateFilter = NumericDocValuesField.newRangeQuery("fetchedAt", lowBound, highBound);
                builder.add(new BooleanClause(dateFilter, FILTER));
            }

            Query queryDescription = parse("description", queryStr);
            Query queryTitle = parse("title", queryStr);

            Query query = builder
                    .add(new BooleanClause(queryDescription, MUST))
                    .add(new BooleanClause(queryTitle, SHOULD))
                    .build();

            TopDocs top = searcher.search(query, limit);
            return of(top.scoreDocs)
                    .map(doc -> {
                        try {
                            return toNews(reader.document(doc.doc), doc.score);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Query parse(String field, String queryStr) throws QueryNodeException {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        try (TokenStream tokens = analyzer.tokenStream(field, queryStr)) {
            CharTermAttribute termAttr = tokens.getAttribute(CharTermAttribute.class);
            tokens.reset();
            while (tokens.incrementToken()) {
                if (!tokens.hasAttribute(CharTermAttribute.class)) {
                    continue;
                }
                Term term = new Term(field, termAttr.toString());
                TermQuery termQuery = new TermQuery(term);
                builder.add(new BooleanClause(termQuery, MUST));
            }
            tokens.end();
        } catch (IOException e) {
            throw new IOError(e);
        }
        return builder.build();
    }

    private Document toDocument(News news) {
        Document doc = new Document();
        doc.add(new TextField("description", news.getDescription(), Field.Store.YES));
        doc.add(new TextField("title", news.getTitle(), Field.Store.YES));
        doc.add(new StringField("author", news.getAuthor(), Field.Store.YES));
        doc.add(new StringField("url", news.getUrl(), Field.Store.YES));
        doc.add(new StringField("urlToImage", news.getUrlToImage(), Field.Store.YES));
        doc.add(new NumericDocValuesField("publishedAt", news.getPublishedAt().getTime()));
        doc.add(new NumericDocValuesField("fetchedAt", news.getFetchedAt().getTime()));
        return doc;
    }

    private News toNews(Document document, float score) {
        return new News(
                document.get("author"),
                document.get("description"),
                getDate(document, "publishedAt"),
                document.get("title"),
                document.get("url"),
                document.get("urlToImage"),
                score,
                getDate(document, "fetchedAt"));
    }

    private Date getDate(Document document, String fieldName) {
        IndexableField field = document.getField(fieldName);
        return new Date(field != null ? field.numericValue().longValue() : 0);
    }


}
