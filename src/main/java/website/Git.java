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

    public Git clone(File other) {
        return clone(other.getAbsolutePath());
    }

    public Git clone(File other, String branch) {
        return clone(other.getAbsolutePath(), branch);
    }

    public Git clone(String other) {
        execute("git", "clone", basePath, other);
        return new Git(other)
                .withDeleteAllOnClose(true);
    }

    public Git clone(String other, String branch) {
        if (branch == null) {
            return clone(other);
        }
        execute("git", "clone", "-b", branch, basePath, other);
        return new Git(other)
                .withDeleteAllOnClose(true);
    }

    public String getBasePath() {
        return basePath;
    }

    public Git add(File file) {
        return add(file.getAbsolutePath());
    }

    private Git add(String file) {
        String relativePath = Paths.get(basePath).relativize(Paths.get(file)).toString();
        execute("git", "add", relativePath);
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
                ProcessBuilder processBuilder = new ProcessBuilder(command)
                        .redirectOutput(err.toFile())
                        .redirectError(err.toFile());
                Process process = assignDirectory(processBuilder)
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

    private ProcessBuilder assignDirectory(ProcessBuilder processBuilder) {
        File baseDir = new File(basePath);
        if (baseDir.isDirectory()) {
            processBuilder.directory(baseDir);
        }
        return processBuilder;
    }

    private int executeWithResult(String ...command) {
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            return assignDirectory(processBuilder)
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

    public void git(String ...args) {
        String []cmd = new String[args.length + 1];
        System.arraycopy(args, 0, cmd, 1, args.length);
        cmd[0] = "git";
        execute(cmd);
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
