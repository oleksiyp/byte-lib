package dbpedia;

import byte_lib.Progress;
import org.iq80.snappy.SnappyFramedOutputStream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class Compressed {
    public static PrintStream snappyPrintStream(String outFile, Progress progress) throws IOException {
        progress.message("Writing '" + outFile + "'");
        return new PrintStream(
                new SnappyFramedOutputStream(
                        new FileOutputStream(outFile)));
    }
}
