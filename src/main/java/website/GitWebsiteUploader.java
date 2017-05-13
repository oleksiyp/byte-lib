package website;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;

public class GitWebsiteUploader implements WebsiteUploader {
    private File dailyFiles;
    private File dailyCatFiles;
    private final Git mainRepo;
    private final Git cacheRepo;
    private final String branch;
    private final String gitRepoDailyPath;
    private String gitRepoDailyCatPath;
    private String commitMessage;
    private String userEmail;
    private String userName;

    public GitWebsiteUploader(Git mainRepo,
                              Git cacheRepo,
                              String branch,
                              String gitRepoDailyPath) {
        this.mainRepo = mainRepo;
        this.cacheRepo = cacheRepo;
        this.branch = branch;
        this.gitRepoDailyPath = gitRepoDailyPath;
    }

    public String getCommitMessage() {
        return commitMessage;
    }

    public void setCommitMessage(String commitMessage) {
        this.commitMessage = commitMessage;
    }

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setGitRepoDailyCatPath(String gitRepoDailyCatPath) {
        this.gitRepoDailyCatPath = gitRepoDailyCatPath;
    }

    public String getGitRepoDailyCatPath() {
        return gitRepoDailyCatPath;
    }

    @Override
    public void setPathToDaily(File dailyFiles) {
        this.dailyFiles = dailyFiles;
    }

    @Override
    public void setPathToDailyCat(File dailyCatFiles) {
        this.dailyCatFiles = dailyCatFiles;
    }

    @Override
    public void update() {
        if (cacheRepo != null) {
            if (cacheRepo.exists()) {
                cacheRepo.pull();
            } else {
                mainRepo.clone(cacheRepo.getBasePath(), branch);
            }
        }

        try (Git tempRepo = cacheRepo != null ?
                cacheRepo :
                mainRepo.clone(tempDir("git-website-uploader-repo"), branch)) {

            if (userEmail != null) {
                tempRepo.git("config", "--global", "user.email", userEmail);
            }
            if (userName != null) {
                tempRepo.git("config", "--global", "user.name", userName);
            }

            Path start = dailyFiles.toPath();
            Files.walkFileTree(start, new CopyAndAddToGit(tempRepo, start, gitRepoDailyPath));
            start = dailyCatFiles.toPath();
            Files.walkFileTree(start, new CopyAndAddToGit(tempRepo, start, gitRepoDailyCatPath));
            if (tempRepo.checkHasChanges()) {
                tempRepo.commit(String.format(
                        commitMessage != null ?
                                commitMessage :
                                "Data update %s", new Date()));
                tempRepo.push();
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    private File tempDir(String prefix) throws IOException {
        return Files.createTempDirectory(prefix).toFile();
    }

    private class CopyAndAddToGit extends SimpleFileVisitor<Path> {
        private final Git tempRepo;
        private final Path start;
        private String destPath;

        public CopyAndAddToGit(Git tempRepo, Path start, String destPath) {
            this.tempRepo = tempRepo;
            this.start = start;
            this.destPath = destPath;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            Path inGitRepo = Paths.get(tempRepo.getBasePath(), destPath)
                    .resolve(start.relativize(file));

            Files.createDirectories(inGitRepo.getParent());

            Files.copy(file, inGitRepo, StandardCopyOption.REPLACE_EXISTING);

            tempRepo.add(inGitRepo.toFile());

            return super.visitFile(file, attrs);
        }
    }
}
