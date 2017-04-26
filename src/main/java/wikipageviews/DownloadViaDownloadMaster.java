package wikipageviews;

import byte_lib.ByteString;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class DownloadViaDownloadMaster {

    public static final ByteString COMMENT_START = ByteString.bs("#");
    private static PrintStream out;

    public static void main(String[] args) throws IOException {
        try (PrintStream out = new PrintStream("dm.txt")) {
            DownloadViaDownloadMaster.out = out;
            ByteString.load("pagviews-2016.txt")
                    .iterate(ByteString.bs("\n"),
                            DownloadViaDownloadMaster::downloadLink);
            ByteString.load("pagviews-2017.txt")
                    .iterate(ByteString.bs("\n"),
                            DownloadViaDownloadMaster::downloadLink);
        }
    }

    private static void downloadLink(ByteString line) {
        if (line.trim().startsWith(COMMENT_START)) {
            return;
        }
        try {
            out.println("http://192.168.0.178:8081/downloadmaster/dm_apply.cgi?action_mode=DM_ADD&download_type=5&again=no&usb_dm_url=" + URLEncoder.encode(line.toString(), "UTF-8") + "&t=0.04847648771947588");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
