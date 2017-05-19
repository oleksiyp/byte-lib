package wikipageviews;

import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Math.E;
import static java.lang.Math.log;
import static java.util.Comparator.comparing;
import static java.util.Comparator.reverseOrder;

public class PageViewRecordCategory {
    private final String category;
    private final String lang;
    private List<PageViewRecord> records;
    private double score;
    private Double newsScore;

    public PageViewRecordCategory() {
        this.lang = "";
        this.category = "";
        this.records = new ArrayList<>();
    }

    public PageViewRecordCategory(String langCategory) {
        int idx = langCategory.indexOf(" ");
        this.lang = idx != -1 ? langCategory.substring(0, idx) : "en";
        this.category = idx != -1 ? langCategory.substring(idx + 1) : langCategory;
        this.records = new ArrayList<>();
    }

    public void add(PageViewRecord record) {
        records.add(record);
    }

    public String getLang() {
        return lang;
    }

    public String getCategory() {
        return category;
    }

    public List<PageViewRecord> getRecords() {
        return records;
    }

    public double getScore() {
        return score;
    }

    public int getScoreRounded() {
        return (int) score;
    }

    public void addAll(List<PageViewRecord> records) {
        this.records.addAll(records);
    }

    public void cutRecords(int maxSize, int maxPosition) {
        records = records.stream()
                .filter(pvr -> pvr.getPosition() < maxPosition)
                .limit(maxSize)
                .collect(Collectors.toList());
    }

    public void sortRecords() {
        records.sort(comparing(PageViewRecord::getScore).reversed());
    }

    public void scoreRecords(int maxSize, int maxPosition) {
        score = records.stream()
                .filter(pvr -> pvr.getPosition() < maxPosition)
                .limit(maxSize)
                .mapToDouble(PageViewRecord::getScore)
                .average()
                .orElse(0.0) * log(E + records.size()) * newsScore;
    }

    public void setNewsScore(Double newsScore) {
        this.newsScore = newsScore;
    }

    public Double getNewsScore() {
        return newsScore;
    }
}
