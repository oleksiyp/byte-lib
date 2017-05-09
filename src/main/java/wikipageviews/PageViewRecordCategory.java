package wikipageviews;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;

public class PageViewRecordCategory {
    public static final String OTHER_CATEGORY = "en Other";

    private final String category;
    private final String lang;
    private List<PageViewRecord> records;
    private double score;

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

    public void sortRecords() {
        records.sort(comparing(PageViewRecord::getScore));
    }

    public List<PageViewRecord> getRecords() {
        return records;
    }

    public double getScore() {
        return score;
    }

    public void addAll(List<PageViewRecord> records) {
        this.records.addAll(records);
    }

    public void cutRecords(int maxSize) {
        records = records.stream()
                .sorted(comparing(PageViewRecord::getScore).reversed())
                .limit(maxSize)
                .collect(Collectors.toList());
    }

    public void scoreRecords(int maxSize) {
        score = records.stream()
                .sorted(comparing(PageViewRecord::getScore))
                .limit(maxSize)
                .mapToDouble(PageViewRecord::getScore)
                .sum();
    }
}
