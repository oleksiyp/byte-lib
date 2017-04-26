package wikipageviews;

import byte_lib.ByteString;

import static byte_lib.ByteString.EMPTY;
import static byte_lib.ByteString.bs;

public class LabelRecordParser {
    public static final LabelRecordParser PARSER = new LabelRecordParser();

    private static final ByteString LABEL_URI = bs("> <http://www.w3.org/2000/01/rdf-schema#label> \"");
    private static final ByteString RESOURCE_AND_LANG_START = bs("<http://");
    private static final ByteString RESOURCE_AND_LANG_MIDDLE = bs(".dbpedia.org/resource/");
    private static final ByteString LANG_PREFIX = bs("\"@");
    // <http://uk.dbpedia.org/resource/Esperanto>
    // <http://www.w3.org/2000/01/rdf-schema#label>
    // "Esperanto"@uk
    // <http://uk.wikipedia.org/wiki/Esperanto?oldid=7186485> .

    public LabelRecord parse(ByteString line) {
        ByteString[] arr = line.split(LABEL_URI);
        if (arr.length < 2) {
            return null;
        }
        ByteString resourceAndLang = arr[0].cut(RESOURCE_AND_LANG_START, EMPTY);
        if (resourceAndLang == null) {
            return null;
        }
        ByteString[] resourceAndLangArr = resourceAndLang.split(RESOURCE_AND_LANG_MIDDLE);
        if (resourceAndLangArr.length < 2) {
            return null;
        }
        ByteString resourceLang = resourceAndLangArr[0];
        ByteString resource = resourceAndLangArr[1];
        ByteString labelAndRest = arr[1];
        int endOfLabelIdx = labelAndRest.lastIndexOf((byte) '\"');
        if (endOfLabelIdx == -1) {
            return null;
        }
        int endOfLangIdx = labelAndRest.indexOf((byte) ' ', endOfLabelIdx);
        if (endOfLangIdx == -1) {
            return null;
        }

        ByteString label = labelAndRest.substring(0, endOfLabelIdx);
        ByteString lang = labelAndRest.substring(endOfLabelIdx, endOfLangIdx)
                .cut(LANG_PREFIX, EMPTY);
        if (lang == null) {
            return null;
        }

        return new LabelRecord(resourceLang, resource, lang, label);
    }
}
