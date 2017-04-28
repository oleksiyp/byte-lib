package byte_lib;

import java.io.PrintStream;

public interface Progress {
    Progress VOID = new VoidProgress();

    void message(String message);

    void reset(long total);

    void progress(long delta);

    static Progress toConsole(PrintStream out) {
        return new ConsoleProgress(out);
    }

    static Progress voidIfNull(Progress progress) {
        if (progress == null) {
            return VOID;
        }
        return progress;
    }

    class ConsoleProgress implements Progress {
        private long total;
        private long progressOut;
        private long outN;
        private long n;
        private long progressDelta;
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
        public void reset(long total) {
            this.total = total;
            progressOut = total / progressDelta;
            outN = 0;
            n = 0;
        }


        @Override
        public void progress(long delta) {
            if (outN >= progressOut && total > 0) {
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
        public void reset(long total) {

        }

        @Override
        public void progress(long delta) {

        }
    }
}
