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
        OBJECT_END,
        SKIP_REST
    }

    public DbpediaTuple parse(ByteString line) {
        reset();

        for (int i = 0; i <= line.length(); i++) {
            byte b = i < line.length() ? line.byteAt(i) : (byte) ' ';
            if (inState(State.SKIP_REST)) {
                // skip
            } else if (inState(State.START) && b == '<') {
                start = i + 1;
                state = State.SUBJECT;
            } else if (inState(State.SUBJECT) && b == '>') {
                subject = line.substring(start, i);
                state = State.SUBJECT_END;
            } else if (inState(State.SUBJECT_END) && b == '<') {
                start = i + 1;
                state = State.PREDICATE;
            } else if (inState(State.PREDICATE) && b == '>') {
                predicate = line.substring(start, i);
                state = State.PREDICATE_END;
            } else if (inState(State.PREDICATE_END) && b == '<') {
                start = i + 1;
                state = State.OBJECT;
            } else if (inState(State.OBJECT) && b == '>') {
                objectIsUri = true;
                object = line.substring(start, i);
                state = State.SKIP_REST;
            } else if (inState(State.PREDICATE_END) && b == '\"') {
                start = i + 1;
                state = State.OBJECT_TEXT;
            } else if (inState(State.OBJECT_TEXT) && b == '\"') {
                objectIsUri = false;
                object = line.substring(start, i);
                state = State.OBJECT_TEXT_END;
            } else if (inState(State.OBJECT_TEXT) && b == '\\') {
                state = State.OBJECT_TEXT_QUOTE;
            } else if (inState(State.OBJECT_TEXT_QUOTE) && b == '\"') {
                state = State.OBJECT_TEXT;
            } else if (inState(State.OBJECT_TEXT_QUOTE) && b == '\\') {
                state = State.OBJECT_TEXT;
            } else if (inState(State.OBJECT_TEXT_END) && b == '@') {
                start = i + 1;
                state = State.OBJECT_TEXT_LANG;
            } else if (inState(State.OBJECT_TEXT_LANG) && isSpaceByte(b)) {
                objectLang = line.substring(start, i);
                state = State.SKIP_REST;
            } else if (inState(State.OBJECT_TEXT_END)) {
                state = State.SKIP_REST;
            } else if (inState(State.SUBJECT) || inState(State.OBJECT) || inState(State.PREDICATE)
                    || inState(State.OBJECT_TEXT) || inState(State.OBJECT_TEXT_LANG)) {
                // skip
            } else if (isSpaceByte(b)) {
                // skip
            } else {
                error = "Error at '" + line.substring(i) + "' in state " + state;
                System.out.println(error);
                return null;
            }
        }
        if (state != State.SKIP_REST) {
            error = "Finished parsing in wrong state " + state + ": " + line;
            System.out.println(error);
            return null;
        }
        return new DbpediaTuple(subject, predicate, objectIsUri, object, objectLang);
    }

    private void reset() {
        state = State.START;
        subject = object = predicate = objectLang = null;
        objectIsUri = false;
        start = end = -1;
        error = null;
    }

    private boolean inState(State state) {
        return this.state == state;
    }

    public String getError() {
        return error;
    }
}
