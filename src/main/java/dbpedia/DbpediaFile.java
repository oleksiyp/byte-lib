package dbpedia;

import byte_lib.ByteString;
import byte_lib.ByteStringInputStream;
import byte_lib.Progress;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;

import static byte_lib.ByteString.bs;
import static byte_lib.ByteStringInputStream.file;

public class DbpediaFile {
    public static final ByteString COMMENT_START = bs("#");

    private File file;

    private Progress progress = Progress.VOID;

    private int linesCount;

    public DbpediaFile(File file) {
        this.file = file;
    }

    public void setProgress(Progress progress) {
        this.progress = progress;
    }

    public Progress getProgress() {
        return progress;
    }

    public DbpediaFile countLines() {
        try {
            progress.message("Counting records in '" + file.getName() + "'");
            setLinesCount(file(file).countLines());
            progress.message(getLinesCount() + " records in '" + file.getName() + "'");
        } catch (IOException ex) {
            throw new IOError(ex);
        }
        return this;
    }

    private static boolean isCommentLine(ByteString s) {
        return s.trim().startsWith(COMMENT_START);
    }

    public <T> void readRecords(Function<ByteString, T> parser, Consumer<T> recordConsumer) {
        if (getLinesCount() != 0) {
            progress.reset(getLinesCount());
        }
        try (ByteStringInputStream in = file(file)) {
            in.readLines((line) -> {
                if (getLinesCount() != 0) {
                    progress.progress(1);
                }
                if (isCommentLine(line)) {
                    return;
                }

                T record = parser.apply(line);

                if (record == null) {
                    return;
                }

                recordConsumer.accept(record);
            });
        } catch (IOException ex) {
            throw new IOError(ex);
        }
    }

    public int getLinesCount() {
        return linesCount;
    }

    public void setLinesCount(int linesCount) {
        this.linesCount = linesCount;
    }

    public ByteString readAll() {
        try {
            return ByteString.load(file, progress, true);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
