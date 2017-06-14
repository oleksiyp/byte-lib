package dbpedia;

import byte_lib.string.ByteString;
import byte_lib.string.ByteStringBuilder;

import static byte_lib.string.ByteString.bs;

public class DbpediaResource {
    public static final ByteString SPACE = bs(" ");
    private final ByteString lang;
    private final ByteString resource;

    public DbpediaResource(ByteString lang, ByteString resource) {
        this.lang = lang;
        this.resource = resource;
    }

    public ByteString getLang() {
        return lang;
    }

    public ByteString getResource() {
        return resource;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DbpediaResource that = (DbpediaResource) o;

        if (!lang.equals(that.lang)) return false;
        return resource.equals(that.resource);
    }

    @Override
    public int hashCode() {
        int result = lang.hashCode();
        result = 31 * result + resource.hashCode();
        return result;
    }

    public ByteString getLangResource() {
        return new ByteStringBuilder()
                .append(lang)
                .append(SPACE)
                .append(resource)
                .build();
    }
}
