package byte_lib;

import static byte_lib.ByteString.SEPARATOR;

public interface IdxMapper {
    long map(ByteString str, long start, long end);

    static long firstField(ByteString str, long start, long end) {
        return str.fieldsIdx(SEPARATOR, start, end, 0, 0);
    }

    static long secondField(ByteString str, long start, long end) {
        return str.fieldsIdx(SEPARATOR, start, end, 1, 1);
    }

    static long thirdField(ByteString str, long start, long end) {
        return str.fieldsIdx(SEPARATOR, start, end, 2,2);
    }

    static long firstTwoFields(ByteString str, long start, long end) {
        return str.fieldsIdx(SEPARATOR, start, end, 0, 1);
    }

    static long firstThreeFields(ByteString str, long start, long end) {
        return str.fieldsIdx(SEPARATOR, start, end, 0, 2);
    }

    static IdxMapper field(int nField) {
        return (str, start, end) -> str.fieldsIdx(SEPARATOR, start, end, nField, nField);
    }

    static IdxMapper fields(int fieldStart, int fieldEnd) {
        return (str, start, end) -> str.fieldsIdx(SEPARATOR, start, end, fieldStart, fieldEnd);
    }

    static IdxMapper field(ByteString separator, int nField) {
        return (str, start, end) -> str.fieldsIdx(separator, start, end, nField, nField);
    }

    static IdxMapper fields(ByteString separator, int fieldStart, int fieldEnd) {
        return (str, start, end) -> str.fieldsIdx(separator, start, end, fieldStart, fieldEnd);
    }

}
