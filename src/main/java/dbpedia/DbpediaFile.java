package dbpedia;

import byte_lib.io.ByteFiles;
import byte_lib.string.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static byte_lib.string.ByteString.NEW_LINE;
import static byte_lib.string.ByteString.bs;

public class DbpediaFile {
    private final static Logger LOG = LoggerFactory.getLogger(DbpediaFile.class);
    public static final ByteString COMMENT_START = bs("#");

    private File file;

    private int linesCount;
    private int fileId;
    private int filesCount;
    private ByteString content;

    public DbpediaFile(File file) {
        this.file = file;
    }

    public DbpediaFile countLines() {
        ByteString allFile = readAll();
        setLinesCount(allFile.howMuch(NEW_LINE));
        return this;
    }

    private static boolean isCommentLine(ByteString s) {
        return s.trim().startsWith(COMMENT_START);
    }

    public void readRecords(Consumer<DbpediaTuple> recordConsumer) {
        DbpediaTupleParser parser = new DbpediaTupleParser();

        LOG.info("Parsing '" + file.getName() + "'");
        readAll().iterate(NEW_LINE, line  -> {
            if (isCommentLine(line)) {
                return;
            }

            DbpediaTuple record = parser.parse(line);

            if (record == null) {
                if (parser.getError() != null) {
                    String error = parser.getError();
                    LOG.info(error);
                }
                return;
            }

            recordConsumer.accept(record);
        });
    }

    public int getLinesCount() {
        return linesCount;
    }

    public void setLinesCount(int linesCount) {
        this.linesCount = linesCount;
    }

    public ByteString readAll() {
        if (content != null) {
            return content;
        }
        return content = ByteFiles.readAll(file);
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


    public void setFilesCount(int filesCount) {
        this.filesCount = filesCount;
    }

    public int getFilesCount() {
        return filesCount;
    }

    public static List<DbpediaFile> dirFiles(File dir) {
        List<DbpediaFile> files = Stream.of(
                Optional.ofNullable(dir.listFiles((d, name) -> name.endsWith(".bz2")))
                        .orElse(new File[0]))
                .filter(File::isFile)
                .map(DbpediaFile::new)
                .collect(Collectors.toList());

        for (int i = 0; i < files.size(); i++) {
            files.get(i).setFileId(i);
            files.get(i).setFilesCount(files.size());
        }

        return files;
    }

    public DbpediaFile reportNFile() {
        LOG.info("File " + (fileId + 1) + " of " + filesCount + ": " + file.getName());
        return this;
    }

    public DbpediaFile recodeSnappy() {
        String name = file.getName();
        int idx = name.lastIndexOf('.');
        if (idx != -1) {
            name = name.substring(0, idx);
        }
        name += ".snappy";
        File snappyFile = new File(file.getParent(), name);
        if (!snappyFile.isFile()) {
            LOG.info("Recoding file to snappy");
            readAll();
            ByteFiles.writeAll(snappyFile, content);
        }
        file = snappyFile;
        return this;
    }
}
