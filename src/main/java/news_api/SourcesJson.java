package news_api;

import java.util.List;

public class SourcesJson {
    private String status;
    private String code;
    private String message;
    private List<SourceJson> sources;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public List<SourceJson> getSources() {
        return sources;
    }

    public void setSources(List<SourceJson> sources) {
        this.sources = sources;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
