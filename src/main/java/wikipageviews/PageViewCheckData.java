package wikipageviews;

import byte_lib.ByteFiles;
import byte_lib.ByteString;
import byte_lib.ByteStringMap;
import byte_lib.Progress;
import dbpedia.ImagesLookup;
import dbpedia.InterlinksLookup;

import java.io.IOException;
import java.nio.file.Paths;

import static byte_lib.ByteString.*;
import static dbpedia.ImagesLookup.IMAGES;
import static java.util.stream.IntStream.range;
import static dbpedia.InterlinksLookup.INTERLINKS;

public class PageViewCheckData {

    public static final ByteString COLON = bs(":");

    public static void main(String[] args) throws IOException {
        Progress progress = Progress.toConsole(System.out);

        ByteString pageviews = ByteFiles.readAll("pageviews-20170418-120000.gz", progress);
        progress.message("Indexing pageviews");
        int []minMax = {Integer.MAX_VALUE, Integer.MIN_VALUE};

        ByteString s[] = {EMPTY};

        InterlinksLookup.init(progress);
        ImagesLookup.init(progress);

        pageviews.iterate(ByteString.NEW_LINE, (pageview) -> {
            ByteString lang = pageview.firstField();
            ByteString resource = pageview.field(1);
            if (INTERLINKS.isMainPage(lang, resource)) {
                return;
            }
            if (!INTERLINKS.hasLang(lang)) {
                return;
            }
            if (INTERLINKS.isSpecial(resource)) {
                return;
            }
            if (INTERLINKS.isTemplate(resource)) {
                return;
            }

            int statCounter = pageview.field(2).toInt();

            minMax[0] = Math.min(statCounter, minMax[0]);
            if (minMax[1] < statCounter) {
                minMax[1] = Math.max(statCounter, minMax[1]);
                s[0] = pageview;
            }
        });
        System.out.println(s[0].toString());

        ByteStringMap<Long> map = new ByteStringMap<>();
        int []pageviewHisto = new int[100];
        int []thumbnailHisto = new int[100];
        MainPageRate rate = new MainPageRate();
        pageviews.iterate(ByteString.NEW_LINE, (pageview) -> {
            ByteString lang = pageview.firstField();
            if (!INTERLINKS.hasLang(lang)) {
                return;
            }

            ByteString resource = pageview.field(1);
            int statCounter = pageview.field(2).toInt();
            if (INTERLINKS.isMainPage(lang, resource)) {
                rate.add(statCounter);
                return;
            }

            if (INTERLINKS.isSpecial(resource)) {
                return;
            }

            if (INTERLINKS.isTemplate(resource)) {
                return;
            }

            map.put(lang, map.getOrDefault(lang, 0L) + 1);

            int h = histoMapping(statCounter, minMax[0], minMax[1], pageviewHisto.length);
            if (IMAGES.getThumbnial(pageview.firstTwoFields()) != null && IMAGES.getDepiction(lang, resource) != null) {
                thumbnailHisto[h]++;
            }
            pageviewHisto[h]++;
        });

        System.out.println(rate.get());
        map.forEach((key, value) -> System.out.println(key + " = " + value));

        range(0, pageviewHisto.length)
                .forEach((idx) -> {
                    long ss = idx * (minMax[1] - minMax[0]) / (pageviewHisto.length - 1) + minMax[0];
                    System.out.println(ss + " " + pageviewHisto[idx] + " " + thumbnailHisto[idx]);
                });
    }

    private static int histoMapping(int val, int min, int max, int length) {
        int r = (val - min) * (length - 1) / (max - min);
        if (r < 0) r = 0;
        if (r > length - 1) r = length - 1;
        return r;
    }

}
