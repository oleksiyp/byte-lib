package website;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;

import static java.util.stream.Collectors.joining;

public class Git implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Git.class);

    private final String basePath;
    private boolean deleteAllOnClose;

    public Git(Path basePath) {
        this(basePath.toFile());
    }

    public Git(File basePath) {
        this(basePath.getAbsolutePath());
    }

    public Git(String basePath) {
        this.basePath = basePath;
    }

    public Git init(boolean bare) {
        if (bare) {
            execute("git", "init", basePath, "--bare");
        } else {
            execute("git", "init", basePath);
        }
        return this;
    }

    public Git clone(Path other) {
        return clone(other.toFile());
    }

    public Git clone(File other) {
        return clone(other.getAbsolutePath());
    }

    public Git clone(String other) {
        execute("git", "clone", basePath, other);
        return new Git(other)
                .withDeleteAllOnClose(true);
    }

    public String getBasePath() {
        return basePath;
    }

    public Git add(Path file) {
        return add(file.toFile());
    }

    private Git add(File file) {
        return add(file.getAbsolutePath());
    }

    private Git add(String file) {
        execute("git", "add", Paths.get(basePath).relativize(Paths.get(file)).toString());
        return this;
    }

    public Git commit(String message) {
        execute("git", "commit", "-m", message);
        return this;
    }

    public Git pull() {
        execute("git", "pull");
        return this;
    }

    public Git push() {
        execute("git", "push");
        return this;
    }

    public boolean checkHasChanges() {
        return executeWithResult("git", "diff", "--quiet", "--cached") == 1;
    }

    private void execute(String ...command) {
        try {
            Path err = Files.createTempFile("git-err", ".txt");
            try {
                Process process = new ProcessBuilder(command)
                        .directory(new File(basePath))
                        .redirectOutput(err.toFile())
                        .redirectError(err.toFile())
                        .start();

                if (LOG.isInfoEnabled()) {
                    LOG.info(Arrays.stream(command).collect(joining(" ")));
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug(Files.readAllLines(err).stream().collect(joining("\n")));
                }
                Files.copy(err, System.out);

                int exitCode = process.waitFor();
                if (exitCode != 0) {
                    throw new RuntimeException(Files.lines(err)
                            .collect(joining("\\n")));
                }
            } finally {
                Files.delete(err);
            }
        } catch (IOException e) {
            throw new IOError(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private int executeWithResult(String ...command) {
        try {
            return new ProcessBuilder(command)
                    .directory(new File(basePath))
                    .start().waitFor();
        } catch (IOException e) {
            throw new IOError(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public Git withDeleteAllOnClose(boolean deleteAllOnClose) {
        this.deleteAllOnClose = deleteAllOnClose;
        return this;
    }

    @Override
    public void close() throws IOException {
        if (deleteAllOnClose) {
            deleteAll();
        }
    }

    public void deleteAll() throws IOException {
        Path base = Paths.get(basePath);
        if (Files.isDirectory(base)) {
            Files.walkFileTree(base, new DeletingFileVisitor());
        }
    }

    public boolean exists() {
        return Files.isDirectory(Paths.get(basePath, ".git"));
    }


    private static class DeletingFileVisitor extends SimpleFileVisitor<Path> {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            Files.delete(file);
            return super.visitFile(file, attrs);
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            Files.delete(dir);
            return super.postVisitDirectory(dir, exc);
        }
    }


}
