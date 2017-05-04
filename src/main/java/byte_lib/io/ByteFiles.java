package byte_lib.io;

import byte_lib.Progress;
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

import static byte_lib.Progress.voidIfNull;
import static byte_lib.string.ByteString.NEW_LINE;
import static java.lang.String.format;

public class ByteFiles {
    public static ByteString readAll(String path) {
        return readAll(new File(path), null);
    }

    public static ByteString readAll(String path, Progress progress) {
        return readAll(new File(path), progress);
    }

    public static ByteString readAll(File file, Progress progress) {
        String name = file.getName();
        progress = voidIfNull(progress);

        long nBytes;
        progress.message(format("Reading '%s'", name));
        try (ByteStringInputStream in = inputStream(file)) {
            nBytes = in.countBytes();
        } catch (IOException ex) {
            throw new IOError(ex);
        }

        progress.message(format("%s to read", Util.sizeToString(nBytes)));
        try (ByteStringInputStream in = inputStream(file)) {
            ByteBuf buf = in.readAll(nBytes, progress);
            return ByteString.bb(buf);
        } catch (IOException ex) {
            throw new IOError(ex);
        }
    }

    public static void readAllLines(File file, Progress progress, Consumer<ByteString> it) {
        ByteString content = readAll(file, progress);
        progress.reset(content.howMuch(NEW_LINE));
        content.iterate(NEW_LINE, (str) -> {
            progress.progress(1);
            it.accept(str);
        });
    }

    public static void writeAll(File file, ByteString str, Progress progress) {
        try (OutputStream out = printStream(file, progress)) {
            str.writeTo(out);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public static PrintStream printStream(File outFile, Progress progress) {
        return printStream(outFile.getPath(), progress);
    }

    public static PrintStream printStream(String outFile, Progress progress) {
        try {
            progress = voidIfNull(progress);
            progress.message("Writing '" + outFile + "'");
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

    public static ByteStringInputStream inputStream(String path) throws IOException {
        return inputStream(new File(path));
    }

    public static ByteStringInputStream inputStream(File file) throws IOException {
        InputStream in = new FileInputStream(file);
        if (file.getName().endsWith(".gz")) {
            in = new GZIPInputStream(in);
        } else if (file.getName().endsWith(".bz2")) {
            in = new BZip2CompressorInputStream(in, true);
        } else if (file.getName().endsWith(".snappy")) {
            in = new SnappyFramedInputStream(in, true);
        }
        return new ByteStringInputStream(in);
    }

    public static void writeMap(File file, ByteStringMap<ByteString> map) {
        try (PrintStream out = printStream(file, null)) {
            map.forEach((lang, obj) -> {
                lang.writeTo(out);
                out.print(' ');
                obj.writeTo(out);
                out.println();
            });
        }
    }

    public static void writeCollection(File file, Collection<? extends ByteString> col) {
        try (PrintStream out = printStream(file, null)) {
            col.forEach((str) -> {
                str.writeTo(out);
                out.println();
            });
        }
    }

    public static void loadCollection(File file, Collection<? super ByteString> col, Progress progress) {
        readAll(file, progress)
                .iterate(NEW_LINE, col::add);
    }

    public static ByteStringMap<ByteString> loadMap(File file,
                                                    Progress progress) {
         return loadMap(file,
                 ByteString::firstField,
                 ByteString::secondField,
                 progress);
    }

    public static <T> ByteStringMap<T> loadMap(File file,
                                               Function<ByteString, ByteString> key,
                                               Function<ByteString, T> value,
                                               Progress progress) {
        ByteString content = readAll(file, progress);
        int total = content.howMuch(NEW_LINE);
        ByteStringMap<T> map = new ByteStringMap<>(total);
        voidIfNull(progress).message("Building ByteStringMap for '" + file.getName() + "' " + total);
        voidIfNull(progress).reset(total);
        content.iterate(NEW_LINE, (str) -> {
            voidIfNull(progress).progress(1);
            map.put(key.apply(str), value.apply(str));
        });
        return map;
    }

    public static ByteStringInputStream inputStreamFromString(String str) {
        return new ByteStringInputStream(
                new ByteArrayInputStream(
                        str.getBytes()));
    }

}
