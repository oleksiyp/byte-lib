package wikipageviews;

import byte_lib.io.ByteFiles;
import byte_lib.string.ByteString;
import byte_lib.io.ByteStringInputStream;
import byte_lib.Progress;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import dbpedia.ImagesLookup;
import dbpedia.InterlinksLookup;
import dbpedia.LabelsLookup;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static byte_lib.io.ByteFiles.inputStream;
import static byte_lib.string.ByteString.bs;
import static dbpedia.ImagesLookup.IMAGES;
import static dbpedia.InterlinksLookup.INTERLINKS;
import static dbpedia.LabelsLookup.LABELS;
import static java.util.Comparator.comparingInt;

public class PageView {
    public static final ByteString COLON = bs(":");
    private static final File OUT_FILE = new File("pageviews-20170418-120000.json");
    public static final String OUT_DIR = "data-res/";
    public static final Pattern FILE_NAME_PATTERN = Pattern.compile("pageviews-(\\d+)-(\\d+).gz");

    private MainPageRate rate;
    private Progress progress;
    private ByteString content;
    public static final int K = 1000;
    private String url;
    private String file;
    private String jsonOut;
    private List<PageViewRecord> topRecords;

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
        } catch (IOException e) {
            throw new IOError(e);
        }

        topRecords = new ArrayList<>();
        while (!topK.isEmpty()) {
            ByteStringPageViewRecord record = topK.remove();
            record.calcScore(rate);
            topRecords.add(record.toJavaStrings());
        }

        Collections.reverse(topRecords);
    }

    public static void initLookups(Progress progress) {
        InterlinksLookup.init(progress);
        ImagesLookup.init(progress);
        LabelsLookup.init(progress);
    }

    public PageView readContent() {
        if (content == null) {
            content = ByteFiles.readAll(file, progress);
        }
        return this;
    }

    public void discardContent() {
        content = null;
    }

    public PageView setProgress(Progress progress) {
        this.progress = progress;
        return this;
    }

    public ByteStringPageViewRecord parseRecord(ByteString pageview) {
        ByteString lang = pageview.firstField();
        if (!INTERLINKS.hasLang(lang)) {
            return null;
        }

        ByteString resource = pageview.field(1);
        int statCounter = pageview.field(2).toInt();
        if (INTERLINKS.isMainPage(lang, resource)) {
            rate.add(statCounter);
            return null;
        }

        if (INTERLINKS.isSpecial(resource) || INTERLINKS.isTemplate(resource)) {
            return null;
        }

        ByteString thumbnail = IMAGES.getThumbnial(pageview.firstTwoFields());
        ByteString depiction = IMAGES.getDepiction(pageview.firstTwoFields());
        ByteString label = LABELS.getLabel(pageview.firstTwoFields());

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

    public static void main(String[] args) throws IOException {
        Progress progress = Progress.toConsole(System.out);
        PageView.initLookups(progress);
        new PageView()
                .setFile("pageviews-20170418-120000.gz")
                .setProgress(progress)
                .readContent()
                .writeTopToJson(K);

    }

    public PageView writeTopToJson(int k) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        File out = new File(getJsonOut());
        if (out.isFile()) {
            topRecords = mapper.readValue(out, new TypeReference<List<PageViewRecord>>(){});
            return this;
        }
        parseRecords(k);
        out.getParentFile().mkdirs();
        mapper.writeValue(
                out,
                topRecords);

        return this;
    }

    public PageView setUrl(String url) {
        this.url = url;
        return this;
    }

    public PageView download(OkHttpClient httpClient) {

        if (new File(file).isFile()) {
            Request request = new Request.Builder()
                    .head()
                    .url(url)
                    .build();
            try {
                long fileLength = new File(file).length();
                Response response = httpClient.newCall(request).execute();
                long contentLen = response.isSuccessful() ? response.body().contentLength() : -1;
                if (fileLength == contentLen) {
                    progress.message("Skipping download " + getFile());
                    return this;
                }
            } catch (IOException e) {
                // skip
            }
        }

        progress.message("Downloading " + getFile());
        for (int i = 1; i <= 10; i++) {
            try {
                Request request = new Request.Builder()
                        .url(url)
                        .build();

                Response response = httpClient.newCall(request).execute();
                if (response.isSuccessful()) {
                    Path out = Paths.get(file);
                    Files.createDirectories(out.getParent());
                    Files.copy(response.body().byteStream(), out,
                            StandardCopyOption.REPLACE_EXISTING);
                    break;
                } else {
                    throw new IOException(response.message());
                }
            } catch (IOException e) {
                System.out.println(e.getMessage());
                try {
                    Files.deleteIfExists(Paths.get(file));
                } catch (IOException e1) {
                    // skip
                }
            }
            System.out.println("Retrying " + i);
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                break;
            }
        }
        return this;
    }

    public String getJsonOut() {
        if (jsonOut == null && file != null) {
            jsonOut = OUT_DIR +
                    new File(file)
                            .getName()
                            .replace(".gz", ".json");
        }
        return jsonOut;
    }

    public void setJsonOut(String jsonOut) {
        this.jsonOut = jsonOut;
    }

    public boolean hasTopRecords() {
        return topRecords != null;
    }

    public List<PageViewRecord> getTopRecords() {
        return topRecords;
    }
}
