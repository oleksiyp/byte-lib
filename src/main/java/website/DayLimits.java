package website;

public class DayLimits {
    private String min;
    private String max;

    public String getMin() {
        return min;
    }

    public void setMin(String min) {
        this.min = min;
    }

    public String getMax() {
        return max;
    }

    public void setMax(String max) {
        this.max = max;
    }

    @Override
    public String toString() {
        return "DayLimits{" +
                "min='" + min + '\'' +
                ", max='" + max + '\'' +
                '}';
    }
}
