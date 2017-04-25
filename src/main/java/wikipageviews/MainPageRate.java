package wikipageviews;

import static byte_lib.ByteString.bs;

public class MainPageRate {
    public static final byte_lib.ByteString MAIN_PAGE_CONCEPT = bs("Main_Page");

    double nViews = 0;
    int n = 0;

    public boolean add(PageViewRecord record) {
        if (!MAIN_PAGE_CONCEPT.equals(record.getConcept())) {
            return false;
        }

        nViews += record.getViews();
        n++;

        return true;
    }

    public boolean isCalculated() {
        return n > 0;
    }

    public double get() {
        return nViews / n;
    }
}
