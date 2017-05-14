package util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;

import static java.util.stream.Collectors.toList;

public class PriorityExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(PriorityExecutor.class);
    private final PriorityQueue<Runnable> queue;
    private final Set<Runnable> locked;
    private final ThreadFactory threadFactory;
    private final Comparator<Runnable> comparator;

    public PriorityExecutor() {
        this(1, Thread::new);
    }

    public PriorityExecutor(int nThreads,
                            ThreadFactory threadFactory) {
        this(nThreads, threadFactory, null);
    }

    public PriorityExecutor(int nThreads,
                            ThreadFactory threadFactory,
                            Comparator<Runnable> comparator) {
        this.threadFactory = threadFactory;
        this.comparator = comparator;
        this.queue = new PriorityQueue<>();
        this.locked = new TreeSet<>();

        initThreads(nThreads);
    }

    private void initThreads(int nThreads) {
        for (int i = 0; i < nThreads; i++) {
            threadFactory.newThread(this::loopForTask).start();
        }
    }

    private void initThread() {
        threadFactory.newThread(this::loopForTask).start();
    }

    private void loopForTask() {
        try {
            while (!Thread.interrupted()) {
                Runnable task;
                synchronized (queue) {
                    while (queue.isEmpty()) {
                        queue.wait();
                    }
                    task = queue.remove();
                }
                try {
                    task.run();
                } catch (VirtualMachineError err) {
                    throw err;
                } catch (Throwable ex) {
                    LOG.error("Error running task {}", task, ex);
                } finally {
                    synchronized (queue) {
                        locked.remove(task);
                    }
                }
            }
        } catch (InterruptedException e) {
            // exit
        }
    }

    public CompletableFuture<Object> execute(Runnable object) {
        synchronized (queue) {
            TaskWrapper wrapper = new TaskWrapper(object, new CompletableFuture<>());

            if (locked.add(wrapper)) {
                queue.add(wrapper);
                queue.notifyAll();
            } else {
                wrapper.done();
            }

            return wrapper.future;
        }
    }

    public CompletableFuture<Void> executeAll(Collection<? extends Runnable> objects) {
        synchronized (queue) {

            return CompletableFuture.allOf(
                    objects.stream()
                    .map(this::execute)
                    .toArray(CompletableFuture[]::new));
        }
    }

    class TaskWrapper implements Runnable, Comparable<TaskWrapper> {
        final Runnable obj;
        final CompletableFuture<Object> future;

        TaskWrapper(Runnable obj, CompletableFuture<Object> future) {
            this.obj = obj;
            this.future = future;
        }

        @Override
        public int compareTo(TaskWrapper o) {
            if (comparator != null) {
                return comparator.compare(obj, o.obj);
            } else if (obj instanceof Comparable){
                Comparable cmp = (Comparable) obj;
                return cmp.compareTo(o.obj);
            } else {
                throw new IllegalArgumentException("no comparator assigned");
            }
        }

        @Override
        public void run() {
            obj.run();
            done();
        }

        public void done() {
            future.complete(null);
        }
    }

}
