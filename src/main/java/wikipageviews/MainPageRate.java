package wikipageviews;

import byte_lib.ByteString;

import static byte_lib.ByteString.bs;

public class MainPageRate {
    public static final ByteString MAIN_PAGE_CONCEPT = bs("Main_Page");

    double nViews = 0;
    int n = 0;

    public void add(int val) {
        nViews += val;
        n++;
    }

    public boolean isCalculated() {
        return n > 0;
    }

    public double get() {
        return nViews / n;
    }

}
