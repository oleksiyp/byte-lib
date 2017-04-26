package wikipageviews;

import byte_lib.ByteString;

import java.io.PrintStream;

class LabelRecord {
    private final ByteString resourceLang;
    private final ByteString resource;
    private final ByteString lang;
    private final ByteString label;

    LabelRecord(ByteString resourceLang, ByteString resource, ByteString lang, ByteString label) {
        this.resourceLang = resourceLang;
        this.resource = resource;
        this.lang = lang;
        this.label = label;
    }

    public void writeTo(PrintStream out) {
        getResourceLang().writeTo(out);
        out.write(' ');
        getResource().writeTo(out);
        out.write(' ');
        getLabel().writeTo(out);
        out.println();
    }

    public ByteString getResourceLang() {
        return resourceLang;
    }

    public ByteString getResource() {
        return resource;
    }

    public ByteString getLang() {
        return lang;
    }

    public ByteString getLabel() {
        return label;
    }
}
