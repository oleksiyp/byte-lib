package website;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;

import static java.nio.file.Files.createTempDirectory;

public class GitWebsiteUploader implements WebsiteUploader {
    public static final String DAILY_FILES = "daily";
    private File dailyFiles;
    private Git mainRepo;
    private Git cacheRepo;

    public GitWebsiteUploader(Git mainRepo, Git cacheRepo) {
        this.mainRepo = mainRepo;
        this.cacheRepo = cacheRepo;
    }

    public GitWebsiteUploader(Git mainRepo) {
        this(mainRepo, null);
    }

    @Override
    public void setPathToDaily(File dailyFiles) {
        this.dailyFiles = dailyFiles;
    }

    @Override
    public void update() {
        if (cacheRepo != null) {
            if (cacheRepo.exists()) {
                cacheRepo.pull();
            } else {
                mainRepo.clone(cacheRepo.getBasePath());
            }
        }

        try (Git tempRepo = cacheRepo != null ?
                cacheRepo :
                mainRepo.clone(createTempDirectory("git-website-uploader-repo"))) {
            Path start = dailyFiles.toPath();
            Files.walkFileTree(start, new CopyAndAddToGit(tempRepo, start));
            if (tempRepo.checkHasChanges()) {
                tempRepo.commit("Updating " + new Date());
                tempRepo.push();
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    private static class CopyAndAddToGit extends SimpleFileVisitor<Path> {
        private final Git tempRepo;
        private final Path start;

        public CopyAndAddToGit(Git tempRepo, Path start) {
            this.tempRepo = tempRepo;
            this.start = start;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            Path inGitRepo = Paths.get(tempRepo.getBasePath(), DAILY_FILES)
                    .resolve(start.relativize(file));

            Files.createDirectories(inGitRepo.getParent());

            Files.copy(file, inGitRepo, StandardCopyOption.REPLACE_EXISTING);

            tempRepo.add(inGitRepo);

            return super.visitFile(file, attrs);
        }
    }
}
