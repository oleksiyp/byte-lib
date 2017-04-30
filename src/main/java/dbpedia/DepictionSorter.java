package dbpedia;

import byte_lib.Progress;

import java.io.IOException;

public class DepictionSorter {
    private static final String OUT_FILE = "depiction_sorted.txt.snappy";

    public static void main(String[] args) throws IOException {
        Progress progress = Progress.toConsole(System.out);
        FileSorter.sortFile(ImageExtractor.OUT_DEPICTION, OUT_FILE, progress);
    }
}
