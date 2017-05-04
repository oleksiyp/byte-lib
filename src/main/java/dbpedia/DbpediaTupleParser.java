package dbpedia;

import byte_lib.string.ByteString;

import static byte_lib.string.ByteString.isSpaceByte;

public class DbpediaTupleParser {
    private State state;

    private ByteString subject;
    private ByteString predicate;
    private boolean objectIsUri;
    private ByteString object;
    private ByteString objectLang;

    private int start, end;
    private String error;

    private enum State {
        START,
        SUBJECT,
        SUBJECT_END,
        PREDICATE,
        PREDICATE_END,
        OBJECT,
        OBJECT_TEXT,
        OBJECT_TEXT_QUOTE,
        OBJECT_TEXT_END,
        OBJECT_TEXT_LANG,
        SKIP_REST
    }

    public DbpediaTuple parse(ByteString line) {
        reset();

        for (int i = 0; i <= line.length(); i++) {
            byte b = i < line.length() ? line.byteAt(i) : (byte) ' ';
            if (in(State.SKIP_REST)) {
                // skip
            } else if (in(State.START) && b == '<') {
                start = i + 1;
                switchTo(State.SUBJECT);
            } else if (in(State.SUBJECT) && b == '>') {
                subject = line.substring(start, i);
                switchTo(State.SUBJECT_END);
            } else if (in(State.SUBJECT_END) && b == '<') {
                start = i + 1;
                switchTo(State.PREDICATE);
            } else if (in(State.PREDICATE) && b == '>') {
                predicate = line.substring(start, i);
                switchTo(State.PREDICATE_END);
            } else if (in(State.PREDICATE_END) && b == '<') {
                start = i + 1;
                switchTo(State.OBJECT);
            } else if (in(State.OBJECT) && b == '>') {
                objectIsUri = true;
                object = line.substring(start, i);
                switchTo(State.SKIP_REST);
            } else if (in(State.PREDICATE_END) && b == '\"') {
                start = i + 1;
                switchTo(State.OBJECT_TEXT);
            } else if (in(State.OBJECT_TEXT) && b == '\"') {
                objectIsUri = false;
                object = line.substring(start, i);
                switchTo(State.OBJECT_TEXT_END);
            } else if (in(State.OBJECT_TEXT) && b == '\\') {
                switchTo(State.OBJECT_TEXT_QUOTE);
            } else if (in(State.OBJECT_TEXT_QUOTE) && b == '\"') {
                switchTo(State.OBJECT_TEXT);
            } else if (in(State.OBJECT_TEXT_QUOTE) && b == '\\') {
                switchTo(State.OBJECT_TEXT);
            } else if (in(State.OBJECT_TEXT_END) && b == '@') {
                start = i + 1;
                switchTo(State.OBJECT_TEXT_LANG);
            } else if (in(State.OBJECT_TEXT_LANG) && isSpaceByte(b)) {
                objectLang = line.substring(start, i);
                switchTo(State.SKIP_REST);
            } else if (in(State.OBJECT_TEXT_END)) {
                switchTo(State.SKIP_REST);
            } else if (in(State.SUBJECT) || in(State.OBJECT) || in(State.PREDICATE)
                    || in(State.OBJECT_TEXT) || in(State.OBJECT_TEXT_LANG)) {
                // skip
            } else if (isSpaceByte(b)) {
                // skip
            } else {
                error = "Error at '" + line.substring(i) + "' in state " + state;
                return null;
            }
        }
        if (state != State.SKIP_REST) {
            error = "Finished parsing in wrong state " + state + ": " + line;
            return null;
        }
        return new DbpediaTuple(subject, predicate, objectIsUri, object, objectLang);
    }

    private void switchTo(State newState) {
        state = newState;
    }

    private void reset() {
        switchTo(State.START);
        subject = object = predicate = objectLang = null;
        objectIsUri = false;
        start = end = -1;
        error = null;
    }

    private boolean in(State state) {
        return this.state == state;
    }

    public String getError() {
        return error;
    }
}
