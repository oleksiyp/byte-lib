package byte_lib;

import rx.Observer;
import rx.observables.SyncOnSubscribe;

import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.Callable;

public class ByteStringLineAdapter extends SyncOnSubscribe<ByteStringLineAdapter.State, ByteString> {
    class State {
        byte []buf = new byte[1024*256];
        InputStream in;
        boolean eof;
        int ptr;
        int left;
    }

    private Callable<InputStream> inputStreamProvider;

    public ByteStringLineAdapter(Callable<InputStream> inputStreamProvider) {
        this.inputStreamProvider = inputStreamProvider;
    }

    @Override
    protected State generateState() {
        return new State();
    }

    @Override
    protected State next(State state, Observer<? super ByteString> observer) {
        if (state == null) {
            return null;
        }

        try {
            if (state.in == null) {
                state.in = inputStreamProvider.call();
            }

            int idx = searchNewLine(state.buf, state.ptr, state.left);
            if (idx != -1) {
                observer.onNext(ByteString.wrap(state.buf, state.ptr, idx - state.ptr));
                state.ptr = skipDoubleNewLine(state.buf, idx, state.left) + 1;
                return state;
            }

            if (state.eof) {
                if (state.ptr < state.left) {
                    observer.onNext(ByteString.wrap(state.buf, state.ptr, state.left - state.ptr));
                }
                observer.onCompleted();
                return null;
            }

            System.arraycopy(state.buf, state.ptr, state.buf, 0, state.left - state.ptr);
            state.left -= state.ptr;
            state.ptr = 0;

            if (state.buf.length == state.left) {
                state.buf = Arrays.copyOf(state.buf, state.buf.length * 2);
            }

            int read = state.in.read(state.buf, state.left, state.buf.length - state.left);
            if (read == 0) {
                return state;
            }
            if (read == -1) {
                read = 0;
                state.eof = true;
            }

            state.left += read;

            return state;
        } catch (Exception e) {
            observer.onError(e);
            return null;
        }
    }

    private int searchNewLine(byte[] buf, int from, int till) {
        for (int i = from; i < till; i++) {
            if (buf[i] == '\n' || buf[i] == '\r') {
                return i;
            }
        }
        return -1;
    }

    private int skipDoubleNewLine(byte[] buf, int from, int till) {
        if (from + 1 < till) {
            if (buf[from] == '\n' && buf[from + 1] == '\r') {
                return from + 1;
            }
            if (buf[from] == '\r' && buf[from + 1] == '\n') {
                return from + 1;
            }
        }
        return from;
    }
}
