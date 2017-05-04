package byte_lib.io;

import static java.lang.String.format;

class Util {
    public static String sizeToString(long bytes) {
        if (bytes > 1024L * 1024 * 1024 * 1024) {
            return format("%.2fTb", bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0));
        } else if (bytes > 1024L * 1024 * 1024) {
            return format("%.2fGb", bytes / (1024.0 * 1024.0 * 1024.0));
        } else if (bytes > 1024L * 1024) {
            return format("%.2fMb", bytes / (1024.0 * 1024.0));
        } else if (bytes > 1024L) {
            return format("%.2fKb", bytes / 1024.0);
        } else {
            return format("%d bytes", bytes);
        }
    }

}
