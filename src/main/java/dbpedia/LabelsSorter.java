package dbpedia;

import byte_lib.Progress;

import java.io.IOException;

public class LabelsSorter {
    private static final String OUT_FILE = "labels_sorted.txt.snappy";

    public static void main(String[] args) throws IOException {
        Progress progress = Progress.toConsole(System.out);
        FileSorter.sortFile(LabelsExtractor.OUT_FILE, OUT_FILE, progress);
    }
}
