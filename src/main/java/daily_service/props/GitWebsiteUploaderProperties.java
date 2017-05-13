package daily_service.props;

public class GitWebsiteUploaderProperties {
    private String repo;
    private String cache;
    private String branch;
    private String dailyDir;
    private String commitMessage;
    private String userName;
    private String userEmail;
    private String dailyCatDir;

    public String getRepo() {
        return repo;
    }

    public void setRepo(String repo) {
        this.repo = repo;
    }

    public String getCache() {
        return cache;
    }

    public void setCache(String cache) {
        this.cache = cache;
    }

    public String getBranch() {
        return branch;
    }

    public void setBranch(String branch) {
        this.branch = branch;
    }

    public String getDailyDir() {
        return dailyDir;
    }

    public void setDailyDir(String dailyDir) {
        this.dailyDir = dailyDir;
    }

    public void setCommitMessage(String commitMessage) {
        this.commitMessage = commitMessage;
    }

    public String getCommitMessage() {
        return commitMessage;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public String getDailyCatDir() {
        return dailyCatDir;
    }

    public void setDailyCatDir(String dailyCatDir) {
        this.dailyCatDir = dailyCatDir;
    }
}
