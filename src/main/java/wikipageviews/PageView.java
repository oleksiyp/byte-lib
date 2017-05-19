package wikipageviews;

import byte_lib.io.ByteStringInputStream;
import byte_lib.string.ByteString;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.ResponseBody;
import dbpedia.DbpediaLookups;
import news_service.News;
import news_service.NewsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static byte_lib.io.ByteFiles.inputStream;
import static java.util.Comparator.comparingInt;

public class PageView {
    private final static Logger LOG = LoggerFactory.getLogger(PageView.class);
    public static final Pattern FILE_NAME_PATTERN = Pattern.compile("pageviews-(\\d+)-(\\d+).gz");


    private MainPageRate rate;
    private String url;
    private String file;
    private String jsonOutDir;
    private String jsonOut;
    private List<PageViewRecord> topRecords;
    private DbpediaLookups lookups;
    private BlackList blackList;

    public PageView() {
        rate = new MainPageRate();
    }

    public void parseRecords(int k) {
        PriorityQueue<ByteStringPageViewRecord> topK;
        topK = new PriorityQueue<>(
                comparingInt(ByteStringPageViewRecord::getStatCounter));

        try (ByteStringInputStream in = inputStream(file)) {
            in.readLines((pageview) -> {
                ByteStringPageViewRecord record = parseRecord(pageview);
                if (record == null) {
                    return;
                }

                topK.add(record);
                if (topK.size() > k) {
                    topK.remove();
                }
            });
        }

        topRecords = new ArrayList<>();
        while (!topK.isEmpty()) {
            ByteStringPageViewRecord record = topK.remove();
            record.calcScore(rate);
            topRecords.add(record
                    .lookupCategories(lookups.getArticleCategoryLookup())
                    .toJavaStrings());
        }

        Collections.reverse(topRecords);
    }

    public PageView setJsonOutDir(String jsonOutDir) {
        this.jsonOutDir = jsonOutDir;
        return this;
    }

    public PageView setLookups(DbpediaLookups lookups) {
        this.lookups = lookups;
        return this;
    }

    public BlackList getBlackList() {
        return blackList;
    }

    public PageView setBlackList(BlackList backlist) {
        this.blackList = backlist;
        return this;
    }

    public ByteStringPageViewRecord parseRecord(ByteString pageview) {
        ByteString lang = pageview.firstField();
        if (!lookups.getInterlinksLookup().hasLang(lang)) {
            return null;
        }

        ByteString resource = pageview.field(1);
        int statCounter = pageview.field(2).toInt();
        if (lookups.getInterlinksLookup().isMainPage(lang, resource)) {
            rate.add(statCounter);
            return null;
        }

        if (lookups.getInterlinksLookup().isSpecial(resource)
                || lookups.getInterlinksLookup().isTemplate(resource)) {
            return null;
        }

        if (blackList != null && blackList.isForbidden(resource)) {
            return null;
        }

        ByteString thumbnail = lookups.getImagesLookup().getThumbnial(pageview.firstTwoFields());
        ByteString depiction = lookups.getImagesLookup().getDepiction(pageview.firstTwoFields());
        ByteString label = lookups.getLabelsLookup().getLabel(pageview.firstTwoFields());

        if (thumbnail == null || depiction == null) {
            return null;
        }

        return new ByteStringPageViewRecord(lang.copyOf(),
                resource.copyOf(),
                statCounter,
                thumbnail.copyOf(),
                depiction.copyOf(),
                label.copyOf());
    }


    public PageView setFile(String file) {
        this.file = file;
        return this;
    }

    public String getFile() {
        return file;
    }

    public String getDay() {
        Matcher matcher = FILE_NAME_PATTERN.matcher(
                new File(file).getName());
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return "--------";
    }

    public Date getDate() {
        try {
            Matcher matcher = FILE_NAME_PATTERN.matcher(
                    new File(file).getName());
            if (matcher.matches()) {
                String day = matcher.group(1) + "T" + matcher.group(2);
                return new SimpleDateFormat("yyyyMMdd'T'hhmmss").parse(day);
            }
        } catch (ParseException e) {
            // skip
        }
        return new Date(0);
    }

    public PageView readOrParse(int k) {
        File out = new File(getJsonOut());
        if (readHourlyJsonOut()) {
            return this;
        }
        LOG.info("Parsing {}", new File(file).getName());
        parseRecords(k);
        return this;
    }

    public PageView write() {
        File out = new File(getJsonOut());
        out.getParentFile().mkdirs();
        LOG.info("Writing top hourly {}", out);
        try {
            new ObjectMapper().writeValue(
                    out,
                    topRecords);
        } catch (IOException ex) {
            throw new IOError(ex);
        }

        return this;
    }

    public boolean readHourlyJsonOut() {
        File out = new File(getJsonOut());
        if (out.isFile()) {
            LOG.info("Reading top hourly records {}", out);
            try {
                topRecords = new ObjectMapper().readValue(out, new TypeReference<List<PageViewRecord>>() {});
            } catch (IOException e) {
                LOG.error("Failed to read JSON", e);
                return false;
            }
            return true;
        }
        return false;
    }

    public PageView setUrl(String url) {
        this.url = url;
        return this;
    }

    public PageView download(OkHttpClient httpClient) {
        if (hasJsonOut()) {
            return this;
        }
        if (new File(file).isFile()) {
            Request request = new Request.Builder()
                    .head()
                    .url(url)
                    .build();
            try {
                long fileLength = new File(file).length();
                Response response = httpClient.newCall(request).execute();
                if (response.isSuccessful()) {
                    try (ResponseBody body = response.body()) {
                        if (fileLength == body.contentLength()) {
                            LOG.info("Skipping download " + getFile());
                            return this;
                        }
                    }
                }
            } catch (IOException e) {
                // skip
            }
        }

        LOG.info("Downloading {}", getFile());
        for (int i = 1; i <= 10; i++) {
            try {
                Request request = new Request.Builder()
                        .url(url)
                        .build();

                Response response = httpClient.newCall(request).execute();
                if (response.isSuccessful()) {
                    Path out = Paths.get(file);
                    Files.createDirectories(out.getParent());
                    try (ResponseBody body = response.body()) {
                        Files.copy(body.byteStream(), out,
                                StandardCopyOption.REPLACE_EXISTING);
                    }
                    break;
                } else {
                    throw new IOException(response.message());
                }
            } catch (IOException e) {
                LOG.info(e.getMessage());
                try {
                    Files.deleteIfExists(Paths.get(file));
                } catch (IOException e1) {
                    // skip
                }
                LOG.warn("Retrying " + i, e);
            }
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                break;
            }
        }
        return this;
    }

    public PageView searchForNews(NewsService newsService, int limit, int daysSimilarity) {
        topRecords.forEach(record -> {
            List<News> news = newsService.search(
                    record.getLabel(),
                    limit,
                    daysSimilarity,
                    getDate());
            record.setNews(news);
        });
        return this;
    }


    public PageView removeDownloaded() {
        new File(file).delete();
        return this;
    }


    public String getJsonOut() {
        if (jsonOut == null && file != null) {
            if (jsonOutDir == null) {
                throw new RuntimeException("set jsonOutDir for PageView first");
            }
            jsonOut = jsonOutDir + "/" +
                    new File(file)
                            .getName()
                            .replace(".gz", ".json");
        }
        return jsonOut;
    }

    public boolean hasTopRecords() {
        return topRecords != null;
    }

    public boolean hasJsonOut() {
        return new File(getJsonOut()).isFile();
    }

    public List<PageViewRecord> getTopRecords() {
        return topRecords;
    }

    public boolean hasFile() {
        return new File(file).isFile();
    }
}
