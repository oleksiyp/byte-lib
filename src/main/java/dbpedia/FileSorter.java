package dbpedia;

import byte_lib.ByteString;
import byte_lib.Progress;
import byte_lib.sort.LongTimSort;

import java.io.IOException;
import java.io.PrintStream;

import static dbpedia.Compressed.snappyPrintStream;

public class FileSorter {
    public static void sortFile(String inFile, String outFile, Progress progress) throws IOException {
        progress = Progress.voidIfNull(progress);

        ByteString inFileContent = ByteString.load(inFile, progress);
        progress.message("Splitting");

        long[] items = inFileContent.splitIdx(ByteString.NEW_LINE);
        progress.message("Sorting " + items.length + " entries");

        LongTimSort.sort(items, 0, items.length, inFileContent::compareByIdx, null, 0, 0);

        progress.reset(items.length);
        try (PrintStream out = snappyPrintStream(outFile, progress)) {
            for (long idx : items) {
                progress.progress(1);
                inFileContent.writeToByIdx(idx, out);
                out.println();
            }
        }
    }
}
