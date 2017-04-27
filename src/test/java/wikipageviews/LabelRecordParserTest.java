package wikipageviews;

import byte_lib.ByteString;
import org.junit.Test;
import dbpedia.LabelRecord;
import dbpedia.LabelRecordParser;

import static byte_lib.ByteString.bs;
import static org.assertj.core.api.Assertions.assertThat;

public class LabelRecordParserTest {

    @Test
    public void test() throws Exception {
        ByteString line = bs("<http://uk.dbpedia.org/resource/Esperanto> <http://www.w3.org/2000/01/rdf-schema#label> \"Esperanto\"@uk <http://uk.wikipedia.org/wiki/Esperanto?oldid=7186485> .");

        LabelRecord record = LabelRecordParser.PARSER.parse(line);

        assertThat(record).isNotNull();
        assertThat(record.getResourceLang()).isEqualTo(bs("uk"));
        assertThat(record.getResource()).isEqualTo(bs("Esperanto"));
        assertThat(record.getLabel()).isEqualTo(bs("Esperanto"));
        assertThat(record.getLang()).isEqualTo(bs("uk"));
    }
}