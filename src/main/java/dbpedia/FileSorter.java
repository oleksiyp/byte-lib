package dbpedia;

import byte_lib.ByteFiles;
import byte_lib.ByteString;
import byte_lib.Progress;
import byte_lib.sort.LongTimSort;

import java.io.IOException;
import java.io.PrintStream;

import static byte_lib.ByteFiles.printStream;

public class FileSorter {
    public static void sortFile(String inFile, String outFile, Progress progress) throws IOException {
        progress = Progress.voidIfNull(progress);

        ByteString inFileContent = ByteFiles.readAll(inFile, progress);
        progress.message("Splitting");

        long[] items = inFileContent.splitIdx(ByteString.NEW_LINE);
        progress.message("Sorting " + items.length + " entries");

        LongTimSort.sort(items, inFileContent::compareByIdx);

        progress.reset(items.length);
        try (PrintStream out = printStream(outFile, progress)) {
            for (long idx : items) {
                progress.progress(1);
                inFileContent.writeToByIdx(idx, out);
                out.println();
            }
        }
    }

}
