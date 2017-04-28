package dbpedia;

import byte_lib.ByteString;

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

    private ByteString dbpediaResource;
    private ByteString dbpediaResourceLang;

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
        return object;
    }

    public ByteString getObjectLang() {
        return objectLang;
    }

    public ByteString getDbpediaResource() {
        if (dbpediaResource == null) {
            lazyParseDbepdiaResource();
        }
        return dbpediaResource;
    }

    public ByteString getDbpediaResourceLang() {
        if (dbpediaResourceLang == null) {
            lazyParseDbepdiaResource();
        }
        return dbpediaResourceLang;
    }

    private void lazyParseDbepdiaResource() {
        if (subject.startsWith(DBPEDIA_RESOURCE_PREFIX)) {
            dbpediaResource = subject.cut(DBPEDIA_RESOURCE_PREFIX, ByteString.EMPTY);
            dbpediaResourceLang = EN;
            return;
        }

        ByteString resource = subject.cut(HTTP_PREFIX, ByteString.EMPTY);
        if (resource == null) {
            return;
        }

        ByteString[] subjArr = resource.split(DBPEDIA_RESOURCE_SUFFIX);
        if (subjArr.length < 2) {
            return;
        }

        dbpediaResourceLang = subjArr[0];
        dbpediaResource = subjArr[1];
    }
}
