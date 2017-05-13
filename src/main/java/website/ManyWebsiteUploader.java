package website;

import java.io.File;
import java.util.List;

import static java.nio.file.Files.createTempDirectory;

public class ManyWebsiteUploader implements WebsiteUploader {
    private final List<WebsiteUploader> uploaders;

    public ManyWebsiteUploader(List<WebsiteUploader> uploaders) {
        this.uploaders = uploaders;
    }

    @Override
    public void setPathToDaily(File dailyFiles) {
        uploaders.forEach(uploader -> uploader.setPathToDaily(dailyFiles));
    }

    @Override
    public void setPathToDailyCat(File dailyCatFiles) {
        uploaders.forEach(uploader -> uploader.setPathToDailyCat(dailyCatFiles));
    }

    @Override
    public void update() {
        uploaders.forEach(WebsiteUploader::update);
    }
}
