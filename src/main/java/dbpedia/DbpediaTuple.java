package dbpedia;

import byte_lib.ByteString;
import byte_lib.ByteStringBuilder;

import static byte_lib.ByteString.bs;

public class DbpediaTuple {
    public static final ByteString DBPEDIA_RESOURCE_PREFIX = bs("http://dbpedia.org/resource/");
    public static final ByteString HTTP_PREFIX = bs("http://");
    public static final ByteString DBPEDIA_RESOURCE_SUFFIX = bs(".dbpedia.org/resource/");
    public static final ByteString EN = bs("en");

    private final ByteString subject;
    private final ByteString predicate;
    private final boolean objectIsUri;
    private final ByteString object;
    private final ByteString objectLang;

    private ByteString dbpediaSubject;
    private ByteString dbpediaSubjectLang;
    private ByteString dbpediaObjectLang;
    private ByteString dbpediaObject;
    private ByteString objectUnquoted;

    public DbpediaTuple(ByteString subject,
                        ByteString predicate,
                        boolean objectIsUri,
                        ByteString object,
                        ByteString objectLang) {


        this.subject = subject;
        this.predicate = predicate;
        this.objectIsUri = objectIsUri;
        this.object = object;
        this.objectLang = objectLang;
    }

    public ByteString getSubject() {
        return subject;
    }

    public ByteString getPredicate() {
        return predicate;
    }

    public boolean isObjectIsUri() {
        return objectIsUri;
    }

    public ByteString getObject() {
        if (objectUnquoted == null) {
            objectUnquoted = buildUnquotedObject();
        }

        return objectUnquoted;
    }

    private ByteString buildUnquotedObject() {
        long len = object.length();
        ByteStringBuilder builder = new ByteStringBuilder((int) len + 5);
        for (int i = 0; i < len; i++) {
            byte b = object.byteAt(i);
            if (b == '\\' && i + 1 < len && object.byteAt(i + 1) == '\"') {
                builder.append((byte) '\"');
                i++;
            } else if (b == '\\' && i + 1 < len && object.byteAt(i + 1) == '\\') {
                builder.append((byte) '\\');
                i++;
            } else {
                builder.append(b);
            }
        }
        return builder.build();
    }

    public ByteString getObjectWithQuotes() {
        return object;
    }

    public ByteString getObjectLang() {
        return objectLang;
    }

    public ByteString getDbpediaSubject() {
        if (dbpediaSubject == null) {
            lazyLoadDbpediaSubject();
        }
        return dbpediaSubject;
    }


    public ByteString getDbpediaSubjectLang() {
        if (dbpediaSubjectLang == null) {
            lazyLoadDbpediaSubject();
        }
        return dbpediaSubjectLang;
    }

    public ByteString getDbpediaObjectLang() {
        if (dbpediaObjectLang == null) {
            lazyLoadDbpediaObject();
        }
        return dbpediaObjectLang;
    }

    public ByteString getDbpediaObject() {
        if (dbpediaObject == null) {
            lazyLoadDbpediaObject();
        }
        return dbpediaObject;
    }

    private void lazyLoadDbpediaSubject() {
        ByteString[] arr = lazyParseDbepdiaResource(subject);
        if (arr != null) {
            dbpediaSubjectLang = arr[0];
            dbpediaSubject = arr[1];
        }
    }

    private void lazyLoadDbpediaObject() {
        ByteString[] arr = lazyParseDbepdiaResource(object);
        if (arr != null) {
            dbpediaObjectLang = arr[0];
            dbpediaObject = arr[1];
        }
    }

    private ByteString[] lazyParseDbepdiaResource(ByteString uri) {
        if (uri.startsWith(DBPEDIA_RESOURCE_PREFIX)) {
            return new ByteString[] {
                    EN,
                    uri.cut(DBPEDIA_RESOURCE_PREFIX, ByteString.EMPTY)
            };
        }

        ByteString resource = uri.cut(HTTP_PREFIX, ByteString.EMPTY);
        if (resource == null) {
            return null;
        }

        ByteString[] subjArr = resource.split(DBPEDIA_RESOURCE_SUFFIX);
        if (subjArr.length < 2) {
            return null;
        }

        return subjArr;
    }
}
