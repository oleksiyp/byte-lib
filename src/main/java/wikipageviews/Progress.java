package wikipageviews;

class Progress {
    private final int progressOut;
    int n = 0, outN = 0;
    final int total;

    Progress(int total) {
        this.total = total;
        progressOut = total / 200;
    }

    public void progress() {
        if (outN == progressOut) {
            System.out.printf("%.2f%%%n", 100.0 * n / total);
            outN = 0;
        }
        outN++;
        n++;
    }
}
