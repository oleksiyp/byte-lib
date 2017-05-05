package daily_service;

public class GitWebsiteUploaderProperties {
    private String repo;
    private String cache;
    private String branch;
    private String dialyDir;

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

    public String getDialyDir() {
        return dialyDir;
    }

    public void setDialyDir(String dialyDir) {
        this.dialyDir = dialyDir;
    }
}
