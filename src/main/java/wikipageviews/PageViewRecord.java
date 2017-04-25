package wikipageviews;

import byte_lib.ByteString;

import java.util.OptionalDouble;

import static byte_lib.ByteString.bs;

public class PageViewRecord {
    public static final ByteString SEPARATOR = bs(" ");
    private final ByteString project;
    private final ByteString title;
    private final int views;
    private final int size;
    private ByteString concept;

    public PageViewRecord(ByteString project, ByteString title, int views, int size) {
        this.project = project;
        this.title = title;
        this.views = views;
        this.size = size;
    }

    public static PageViewRecord valueOf(ByteString record) {
        ByteString[] text = record.split(SEPARATOR);
        return new PageViewRecord(text[0].copyOf(),
                text[1].copyOf(),
                text[2].toInt(),
                text[3].toInt());
    }

    public OptionalDouble getScore(MainPageRate mainPageRate) {
        if (mainPageRate.isCalculated()) {
            return OptionalDouble.of(100.0 * getViews() / mainPageRate.get());
        } else {
            return OptionalDouble.empty();
        }
    }

    public ByteString getFullTitle() {
        return getProject()
                .append(PageViewsTopExtractor.SEPARATOR2)
                .append(getTitle());
    }

    public ByteString getProject() {
        return project;
    }

    public ByteString getTitle() {
        return title;
    }

    public int getViews() {
        return views;
    }

    public int getSize() {
        return size;
    }

    public ByteString getConcept() {
        return concept;
    }

    public void setConcept(ByteString concept) {
        this.concept = concept;
    }


    public ByteString format() {
        return project
                .append(SEPARATOR)
                .append(title)
                .append(SEPARATOR)
                .append(bs("" + views))
                .append(SEPARATOR)
                .append(bs("" + size));
    }

    @Override
    public String toString() {
        return "PageViewRecord{" +
                "chapter='" + project + '\'' +
                ", item='" + title + '\'' +
                ", views=" + views +
                ", hosts=" + size +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PageViewRecord record = (PageViewRecord) o;

        if (views != record.views) return false;
        if (size != record.size) return false;
        if (!project.equals(record.project)) return false;
        return title.equals(record.title);
    }

    @Override
    public int hashCode() {
        int result = project.hashCode();
        result = 31 * result + title.hashCode();
        result = 31 * result + views;
        result = 31 * result + size;
        return result;
    }
}
