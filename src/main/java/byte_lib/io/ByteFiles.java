package byte_lib.io;

import byte_lib.hashed.ByteStringMap;
import byte_lib.string.ByteString;
import byte_lib.string.buf.ByteBuf;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.iq80.snappy.SnappyFramedInputStream;
import org.iq80.snappy.SnappyFramedOutputStream;

import java.io.*;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static byte_lib.string.ByteString.NEW_LINE;
import static java.lang.String.format;

public class ByteFiles {
    public static ByteString readAll(String path) {
        return readAll(new File(path));
    }

    public static ByteString readAll(File file) {
        long nBytes;
        try (ByteStringInputStream in = inputStream(file)) {
            nBytes = in.countBytes();
        } catch (IOException ex) {
            throw new IOError(ex);
        }

        try (ByteStringInputStream in = inputStream(file)) {
            ByteBuf buf = in.readAll(nBytes);
            return ByteString.bb(buf);
        } catch (IOException ex) {
            throw new IOError(ex);
        }
    }

    public static void readAllLines(File file, Consumer<ByteString> it) {
        ByteString content = readAll(file);
        content.iterate(NEW_LINE, it::accept);
    }

    public static void writeAll(File file, ByteString str) {
        try (OutputStream out = printStream(file)) {
            str.writeTo(out);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public static PrintStream printStream(File outFile) {
        return printStream(outFile.getPath());
    }

    public static PrintStream printStream(String outFile) {
        try {
            OutputStream out = new FileOutputStream(outFile);
            if (outFile.endsWith(".snappy")) {
                out = new SnappyFramedOutputStream(out);
            } else if (outFile.endsWith(".gz")) {
                out = new GZIPOutputStream(out);
            } else if (outFile.endsWith(".bz2")) {
                out = new BZip2CompressorOutputStream(out);
            }
            return new PrintStream(out);
        } catch (IOException ex) {
            throw new IOError(ex);
        }
    }

    public static ByteStringInputStream inputStream(String path) {
        return inputStream(new File(path));
    }

    public static ByteStringInputStream inputStream(File file) {
        try {
            InputStream in = new FileInputStream(file);
            if (file.getName().endsWith(".gz")) {
                in = new GZIPInputStream(in);
            } else if (file.getName().endsWith(".bz2")) {
                in = new BZip2CompressorInputStream(in, true);
            } else if (file.getName().endsWith(".snappy")) {
                in = new SnappyFramedInputStream(in, true);
            }
            return new ByteStringInputStream(in);
        } catch (IOException ex) {
            throw new IOError(ex);
        }
    }

    public static String nonArchivedName(String path) {
        int ptr = path.lastIndexOf('.');
        if (ptr == -1) {
            return path;
        }

        String ext = path.substring(ptr);
        String withoutExt = path.substring(0, ptr);

        if (ext.equals(".snappy") || ext.equals(".bz2") || ext.equals(".gz")) {
            return withoutExt;
        }
        return path;
    }

    public static void writeMap(File file, ByteStringMap<ByteString> map) {
        try (PrintStream out = printStream(file)) {
            map.forEach((lang, obj) -> {
                lang.writeTo(out);
                out.print(' ');
                obj.writeTo(out);
                out.println();
            });
        }
    }

    public static void writeCollection(File file, Collection<? extends ByteString> col) {
        try (PrintStream out = printStream(file)) {
            col.forEach((str) -> {
                str.writeTo(out);
                out.println();
            });
        }
    }

    public static void loadCollection(File file, Collection<? super ByteString> col) {
        readAll(file)
                .iterate(NEW_LINE, col::add);
    }

    public static ByteStringMap<ByteString> loadMap(File file) {
         return loadMap(file,
                 ByteString::firstField,
                 ByteString::secondField
         );
    }

    public static <T> ByteStringMap<T> loadMap(File file,
                                               Function<ByteString, ByteString> key,
                                               Function<ByteString, T> value) {
        ByteString content = readAll(file);
        int total = content.howMuch(NEW_LINE);
        ByteStringMap<T> map = new ByteStringMap<>(total);
        content.iterate(NEW_LINE, (str) ->
                map.put(key.apply(str), value.apply(str)));
        return map;
    }

    public static ByteStringInputStream inputStreamFromString(String str) {
        return new ByteStringInputStream(
                new ByteArrayInputStream(
                        str.getBytes()));
    }

}
