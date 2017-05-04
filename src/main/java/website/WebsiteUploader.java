package website;

import java.io.File;

public interface WebsiteUploader {
    void setPathToDaily(File dailyFiles);

    void update();
}
