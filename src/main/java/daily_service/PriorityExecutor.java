package daily_service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ThreadFactory;

public class PriorityExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(PriorityExecutor.class);
    private final PriorityQueue<Runnable> queue;
    private final ThreadFactory threadFactory;

    public PriorityExecutor() {
        this(1, Thread::new);
    }

    public PriorityExecutor(int nThreads,
                            ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
        this.queue = new PriorityQueue<>();
        initThreads(nThreads);
    }

    public PriorityExecutor(int nThreads,
                            ThreadFactory threadFactory,
                            Comparator<Runnable> comparator) {
        this.threadFactory = threadFactory;
        this.queue = new PriorityQueue<>(comparator);
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
                    LOG.debug("Error running task {}", task, ex);
                }
            }
        } catch (InterruptedException e) {
            // exit
        }
    }

    public void execute(Runnable object) {
        synchronized (queue) {
            queue.add(object);
            queue.notifyAll();
        }
    }

    public void executeAll(Collection<? extends Runnable> objects) {
        synchronized (queue) {
            queue.addAll(objects);
            queue.notifyAll();
        }
    }


}
