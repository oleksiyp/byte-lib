package dbpedia;

import byte_lib.ByteString;
import byte_lib.ByteStringInputStream;
import byte_lib.Progress;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static byte_lib.ByteString.bs;
import static byte_lib.ByteStringInputStream.file;

public class DbpediaFile {
    public static final ByteString COMMENT_START = bs("#");

    private File file;

    private Progress progress = Progress.VOID;

    private int linesCount;
    private int fileId;
    private int filesCount;

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

    public void readRecords(Consumer<DbpediaTuple> recordConsumer) {
        DbpediaTupleParser parser = new DbpediaTupleParser();
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

                DbpediaTuple record = parser.parse(line);

                if (record == null) {
                    if (parser.getError() != null) {
                        String error = parser.getError();
                        progress.message(error);
                    }
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
            return ByteString.load(file, progress);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public File getFile() {
        return file;
    }

    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    public int getFileId() {
        return fileId;
    }

    public static List<DbpediaFile> dirFiles(File dir, Progress progress) {
        List<DbpediaFile> files = Stream.of(
                Optional.ofNullable(dir.listFiles()).orElse(new File[0]))
                .filter(File::isFile)
                .map(DbpediaFile::new)
                .peek((f) -> f.setProgress(Progress.voidIfNull(progress)))
                .collect(Collectors.toList());

        for (int i = 0; i < files.size(); i++) {
            files.get(i).setFileId(i);
            files.get(i).setFilesCount(files.size());
        }

        return files;
    }

    public DbpediaFile reportNFile() {
        progress.message("File " + (fileId + 1) + " of " + filesCount + ": " + file.getName());

        return this;
    }

    public void setFilesCount(int filesCount) {
        this.filesCount = filesCount;
    }

    public int getFilesCount() {
        return filesCount;
    }
}
