package dbpedia;

import byte_lib.Progress;
import byte_lib.sort.FileSorter;

import java.io.IOException;

public class ThumbnailSorter {
    private static final String OUT_FILE = "thumbnail_sorted.txt.snappy";

    public static void main(String[] args) throws IOException {
        Progress progress = Progress.toConsole(System.out);
        FileSorter.sortFile(ImageExtractor.OUT_THUMBNAIL, OUT_FILE, progress);
    }
}
