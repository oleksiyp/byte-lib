package byte_lib;

import java.io.PrintStream;

public interface Progress {
    Progress VOID = new VoidProgress();

    void message(String message);

    void reset(int total);

    void progress(int delta);

    static Progress toConsole(PrintStream out) {
        return new ConsoleProgress(out);
    }

    static Progress voided(Progress progress) {
        if (progress == null) {
            return VOID;
        }
        return progress;
    }

    class ConsoleProgress implements Progress {
        private int total;
        private int progressOut;
        private int outN;
        private int n;
        private int progressDelta;
        private PrintStream out;

        public ConsoleProgress(PrintStream out) {
            this.out = out;
            this.progressDelta = 200;
        }

        @Override
        public void message(String message) {
            out.println(message);
        }

        @Override
        public void reset(int total) {
            this.total = total;
            progressOut = total / progressDelta;
            outN = 0;
            n = 0;
        }


        @Override
        public void progress(int delta) {
            if (outN >= progressOut) {
                out.printf("%.2f%n", 100.0 * n / total);
                outN = 0;
            }
            outN += delta;
            n += delta;
        }
    }

    class VoidProgress implements Progress {

        @Override
        public void message(String message) {

        }

        @Override
        public void reset(int total) {

        }

        @Override
        public void progress(int delta) {

        }
    }
}
