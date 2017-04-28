package dbpedia;

import byte_lib.Progress;
import byte_lib.sort.FileSorter;

import java.io.IOException;

public class DepictionsSorter {
    private static final String OUT_FILE = "depictions_sorted.txt.snappy";

    public static void main(String[] args) throws IOException {
        Progress progress = Progress.toConsole(System.out);
        FileSorter.sortFile(ImageExtractor.OUT_DEPICTION, OUT_FILE, progress);
    }
}
