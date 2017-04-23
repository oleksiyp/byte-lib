package wikipageviews;

import byte_lib.ByteString;

import static byte_lib.ByteString.bs;

public class InterlinkRecordParser {
    private static final ByteString SEPARATOR = bs("> <");
    private static final ByteString TOPIC_START = bs("<http://dbpedia.org/resource/");
    private static final ByteString LANG_INTL_TOPIC_START = bs("http://");
    private static final ByteString LANG_INTL_TOPIC_MIDDLE = bs(".dbpedia.org/resource/");
    public static final ByteString TABLE_SEPARATOR1 = bs(" ");
    public static final String TABLE_SEPARATOR2 = ":";

    public static InterlinkRecord parse(ByteString line) {
        ByteString[] arr = line.split(SEPARATOR);
        ByteString topic = arr[0].cut(TOPIC_START, ByteString.EMPTY);
        if (topic == null) {
            return null;
        }
        ByteString langIntlTopic = arr[2].cut(LANG_INTL_TOPIC_START, ByteString.EMPTY);
        if (langIntlTopic == null) {
            return null;
        }
        int langIdx = langIntlTopic.indexOf((byte) '.');
        if (langIdx == -1) {
            return null;
        }
        ByteString lang = langIntlTopic.substring(0, langIdx);
        ByteString intlTopic = langIntlTopic.substring(langIdx)
                .cut(LANG_INTL_TOPIC_MIDDLE, ByteString.EMPTY);
        if (intlTopic == null) {
            return null;
        }

        return new InterlinkRecord(topic, lang, intlTopic);
    }

    public static InterlinkRecord parseTable(ByteString bs) {
        ByteString[] arr = bs.split(TABLE_SEPARATOR1);
        ByteString[] arr2 = arr[0].split(bs(TABLE_SEPARATOR2));
        return new InterlinkRecord(arr[1].copyOf(), arr2[0].copyOf(), arr2[1].copyOf());
    }
}
