package website;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import static java.nio.file.Files.*;
import static org.assertj.core.api.Assertions.assertThat;

public class GitWebsiteUploaderTest {
    private Git mainRepo;

    @Before
    public void setUp() throws Exception {
        mainRepo = new Git(createTempDirectory("git-website-uploader-repo").toFile());
        mainRepo.init(true);
    }

    @After
    public void tearDown() throws Exception {
        mainRepo.deleteAll();
    }

    @Test
    public void testUpload() throws Exception {
        WebsiteUploader uploader = new GitWebsiteUploader(mainRepo, null, null, "daily", props.getCommitMessage());

        Path path = createTempDirectory("git-website-uploader");
        write(path.resolve("file1.txt"), "value1\nline2\n".getBytes());
        write(path.resolve("file2.txt"), "value2\nline3\n".getBytes());
        write(path.resolve("file3.txt"), "value3\nline4\n".getBytes());

        try {

            uploader.setPathToDaily(path.toFile());

            uploader.update();

            try (Git testRepo = mainRepo.clone(createTempDirectory(
                    "git-website-repo-test").toFile())) {

                Path actualDailyFiles = Paths.get(testRepo.getBasePath(), "daily");
                assertThat(readAllLines(
                        actualDailyFiles.resolve("file1.txt")))
                        .containsExactly("value1", "line2");

                assertThat(readAllLines(
                        actualDailyFiles.resolve("file2.txt")))
                        .containsExactly("value2", "line3");

                assertThat(readAllLines(
                        actualDailyFiles.resolve("file3.txt")))
                        .containsExactly("value3", "line4");
            }
        } finally {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    public void testUploadWithCache() throws Exception {
        try (Git cacheRepo = new Git(createTempDirectory("git-website-cache").toFile())
                .withDeleteAllOnClose(true)) {
            WebsiteUploader uploader = new GitWebsiteUploader(mainRepo, cacheRepo, null, "daily", props.getCommitMessage());

            Path path = createTempDirectory("git-website-uploader");
            write(path.resolve("file1.txt"), "value1\nline2\n".getBytes());
            write(path.resolve("file2.txt"), "value2\nline3\n".getBytes());
            write(path.resolve("file3.txt"), "value3\nline4\n".getBytes());

            try {
                uploader.setPathToDaily(path.toFile());

                uploader.update();

                write(path.resolve("file2.txt"), "value1\nline5\n".getBytes());

                uploader.update();

                write(path.resolve("file2.txt"), "value5\nline6\n".getBytes());

                uploader.update();

                write(path.resolve("file2.txt"), "value5\nline6\n".getBytes());

                uploader.update();

                try (Git testRepo = mainRepo.clone(createTempDirectory(
                        "git-website-repo-test").toFile())) {
                    Path actualDailyFiles = Paths.get(testRepo.getBasePath(), "daily");
                    assertThat(readAllLines(
                            actualDailyFiles.resolve("file1.txt")))
                            .containsExactly("value1", "line2");

                    assertThat(readAllLines(
                            actualDailyFiles.resolve("file2.txt")))
                            .containsExactly("value5", "line6");

                    assertThat(readAllLines(
                            actualDailyFiles.resolve("file3.txt")))
                            .containsExactly("value3", "line4");
                }

            } finally {
                Files.walk(path)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }
    }
}