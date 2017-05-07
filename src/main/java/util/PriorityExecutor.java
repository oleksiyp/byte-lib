package util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

public class PriorityExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(PriorityExecutor.class);
    private final PriorityQueue<Runnable> queue;
    private final Set<Runnable> locked;
    private final ThreadFactory threadFactory;

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

        this.queue = comparator != null
                ? new PriorityQueue<>(comparator)
                : new PriorityQueue<>();

        this.locked = comparator != null
                ? new TreeSet<>(comparator)
                : new TreeSet<>();

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
                    locked.add(task);
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

    public void execute(Runnable object) {
        synchronized (queue) {
            if (locked.contains(object)) {
                return;
            }
            queue.add(object);
            queue.notifyAll();
        }
    }

    public void executeAll(Collection<? extends Runnable> objects) {
        synchronized (queue) {
            List<? extends Runnable> objectsFiltered = objects.stream()
                    .filter(o -> !locked.contains(o))
                    .collect(Collectors.toList());

            if (objectsFiltered.isEmpty()) {
                return;
            }
            queue.addAll(objectsFiltered);
            queue.notifyAll();
        }
    }


}
