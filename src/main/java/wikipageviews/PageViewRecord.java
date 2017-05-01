package wikipageviews;

import byte_lib.ByteString;

import java.util.OptionalDouble;

import static byte_lib.ByteString.bs;

public class PageViewRecord {
    private final ByteString lang;
    private final ByteString resource;
    private final int statCounter;
    private double score;
    private final ByteString thumbnail;
    private final ByteString depiction;
    private final ByteString label;

    public PageViewRecord(ByteString lang,
                          ByteString resource,
                          int statCounter,
                          ByteString thumbnail,
                          ByteString depiction,
                          ByteString label) {
        this.lang = lang;
        this.resource = resource;
        this.statCounter = statCounter;
        this.thumbnail = thumbnail;
        this.depiction = depiction;
        this.label = label;
    }

    public void calcScore(MainPageRate mainPageRate) {
        if (mainPageRate.isCalculated()) {
            score = 100.0 * statCounter / mainPageRate.get();
        } else {
            score = 0.0;
        }
    }

    public String getLang() {
        return lang.toString();
    }

    public String getResource() {
        return resource.toString();
    }

    public int getStatCounter() {
        return statCounter;
    }

    public String getThumbnail() {
        return thumbnail.toString();
    }

    public String getDepiction() {
        return depiction.toString();
    }

    public String getLabel() {
        return label.toString();
    }

    public double getScore() {
        return score;
    }


}
