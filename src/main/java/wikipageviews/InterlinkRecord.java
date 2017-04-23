package wikipageviews;

import byte_lib.ByteString;

public class InterlinkRecord {

    private final ByteString topic;
    private final ByteString lang;
    private final ByteString intlTopic;

    public InterlinkRecord(ByteString topic, ByteString lang, ByteString intlTopic) {
        this.topic = topic;
        this.lang = lang;
        this.intlTopic = intlTopic;
    }

    public ByteString getTopic() {
        return topic;
    }

    public ByteString getLang() {
        return lang;
    }

    public ByteString getIntlTopic() {
        return intlTopic;
    }
}
