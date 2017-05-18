package util;

import java.io.IOError;
import java.io.IOException;

public class IOUtils {
    public static void wrapIOException(BlockWithIOException block) {
        try {
            block.run();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public interface BlockWithIOException {
        void run() throws IOException;
    }
}
