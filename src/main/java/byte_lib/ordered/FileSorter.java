package byte_lib.ordered;

import byte_lib.io.ByteFiles;
import byte_lib.string.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;

import static byte_lib.io.ByteFiles.printStream;

public class FileSorter {
    private final static Logger LOG = LoggerFactory.getLogger(FileSorter.class);

    public static void sortFile(String inFile, String outFile) throws IOException {
        ByteString inFileContent = ByteFiles.readAll(inFile);
        LOG.info("Splitting lines");

        long[] items = inFileContent.splitIdx(ByteString.NEW_LINE);
        LOG.info("Sorting {} entries", items.length);

        LongTimSort.sort(items, inFileContent::compareByIdx);

        try (PrintStream out = printStream(outFile)) {
            for (long idx : items) {
                inFileContent.writeToByIdx(idx, out);
                out.println();
            }
        }
    }

}
