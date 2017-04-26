package byte_lib;

import static java.lang.String.format;

public class Bytes {
    public static String sizeToString(int bytes) {
        if (bytes > 1024 * 1024 * 1024) {
            return format("%.2fGb", bytes / (1024.0 * 1024.0 * 1024.0));
        } else if (bytes > 1024 * 1024) {
            return format("%.2fMb", bytes / (1024.0 * 1024.0));
        } else if (bytes > 1024) {
            return format("%.2fKb", bytes / 1024.0);
        } else {
            return format("%d bytes", bytes);
        }
    }

}
