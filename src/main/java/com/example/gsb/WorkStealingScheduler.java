package com.example.gsb;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A work-stealing task scheduler implemented from scratch.
 *
 * <ul>
 *   <li>Every worker owns a deque. The owner takes its own tasks from the
 *       head (LIFO for forked continuations, depth-first locality); idle
 *       workers steal from other workers' tails (FIFO, stealing the
 *       oldest, likely largest chunk of work).</li>
 *   <li>External submissions are distributed round-robin and appended to
 *       the chosen worker's tail.</li>
 *   <li>When a task running on a thief forks continuations, those
 *       continuations are pushed to the <em>home</em> worker of the task
 *       (the worker whose queue the task originally belonged to), so
 *       stealing does not disturb local ordering semantics.</li>
 *   <li>Idle workers first scan the other deques; if every deque is empty
 *       they park on a condition with exponential back-off instead of busy
 *       spinning.</li>
 *   <li>The worker count can be changed at runtime. Retiring workers drain
 *       their remaining tasks onto a surviving worker before exiting, so
 *       no pending task is lost.</li>
 *   <li>Exceptions thrown by a task are captured (exposed via
 *       {@link #getFailures()} and via the returned future) and never
 *       affect the worker thread or any other task.</li>
 * </ul>
 */
public final class WorkStealingScheduler implements AutoCloseable {

    private static final long MIN_BACKOFF_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long MAX_BACKOFF_NANOS = TimeUnit.MILLISECONDS.toNanos(32);

    /** A failed task together with the exception it threw. */
    public record TaskFailure(String task, Throwable error) { }

    private final class Task {
        final String description;
        final Runnable action;
        volatile Worker home;
        final CompletableFuture<Void> future = new CompletableFuture<>();

        Task(String description, Runnable action) {
            this.description = description;
            this.action = action;
        }
    }

    private final class Worker implements Runnable {
        private final int id;
        private final LinkedBlockingDeque<Task> deque = new LinkedBlockingDeque<>();
        private volatile boolean retiring;
        private volatile Thread thread;

        Worker(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            long backoffNanos = MIN_BACKOFF_NANOS;
            try {
                while (true) {
                    if (retiring) {
                        drainToOthers();
                        synchronized (this) {
                            if (deque.isEmpty()) {
                                return;
                            }
                        }
                        continue;
                    }

                    Task task = deque.pollFirst();
                    boolean stolen = false;
                    if (task == null) {
                        task = stealFromOthers(this);
                        stolen = task != null;
                    }

                    if (task != null) {
                        backoffNanos = MIN_BACKOFF_NANOS;
                        executeTask(this, task, stolen);
                    } else {
                        if (shutdown && pendingCount.get() == 0L) {
                            return;
                        }
                        awaitWork(backoffNanos);
                        backoffNanos = Math.min(backoffNanos * 2L, MAX_BACKOFF_NANOS);
                    }
                }
            } finally {
                workers.remove(this);
            }
        }

        private void drainToOthers() {
            Task task;
            while ((task = deque.pollFirst()) != null) {
                Worker target = pickLiveWorker(this);
                if (target == null) {
                    deque.addFirst(task);
                    return;
                }
                target.deque.addLast(task);
            }
            signalWork();
        }
    }

    private final CopyOnWriteArrayList<Worker> workers = new CopyOnWriteArrayList<>();
    private final ConcurrentLinkedQueue<TaskFailure> failures = new ConcurrentLinkedQueue<>();

    private final AtomicInteger nextWorkerId = new AtomicInteger();
    private final AtomicInteger roundRobin = new AtomicInteger();
    private final AtomicLong taskSequence = new AtomicLong();

    private final AtomicLong pendingCount = new AtomicLong();
    private final AtomicLong executedCount = new AtomicLong();
    private final AtomicLong stolenCount = new AtomicLong();

    private final ReentrantLock idleLock = new ReentrantLock();
    private final Condition workAvailable = idleLock.newCondition();
    private final Object quiescenceMonitor = new Object();

    private volatile boolean shutdown;

    private final ThreadLocal<Worker> currentWorker = new ThreadLocal<>();
    private final ThreadLocal<Task> currentTask = new ThreadLocal<>();

    public WorkStealingScheduler(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be >= 1");
        }
        for (int i = 0; i < parallelism; i++) {
            startWorker();
        }
    }

    private Worker startWorker() {
        Worker worker = new Worker(nextWorkerId.getAndIncrement());
        workers.add(worker);
        Thread thread = new Thread(worker, "ws-worker-" + worker.id);
        thread.setDaemon(true);
        worker.thread = thread;
        thread.start();
        return worker;
    }

    /**
     * Submits a task. Called from a worker thread the task is treated as a
     * continuation: it is pushed to the head of the home worker's deque.
     * Called from outside it is scheduled round-robin on the tail.
     */
    public CompletableFuture<Void> submit(Runnable action) {
        return submit("task-" + taskSequence.incrementAndGet(), action);
    }

    public CompletableFuture<Void> submit(String description, Runnable action) {
        Objects.requireNonNull(action, "action");
        Objects.requireNonNull(description, "description");
        if (shutdown) {
            throw new RejectedExecutionException("scheduler is shut down");
        }
        Worker submitter = currentWorker.get();
        if (submitter != null) {
            Task parent = currentTask.get();
            Worker home = parent != null && parent.home != null ? parent.home : submitter;
            return enqueueLocal(home, description, action);
        }
        return enqueueExternal(null, description, action);
    }

    /**
     * Submits a task from outside directly into a specific live worker's
     * deque (indexed modulo the current live worker count).
     */
    public CompletableFuture<Void> submitOn(int workerIndex, Runnable action) {
        return submitOn(workerIndex, "task-" + taskSequence.incrementAndGet(), action);
    }

    public CompletableFuture<Void> submitOn(int workerIndex, String description, Runnable action) {
        Objects.requireNonNull(action, "action");
        if (shutdown) {
            throw new RejectedExecutionException("scheduler is shut down");
        }
        List<Worker> live = liveWorkers();
        if (live.isEmpty()) {
            throw new RejectedExecutionException("no live workers");
        }
        Worker target = live.get(Math.floorMod(workerIndex, live.size()));
        return enqueueExternal(target, description, action);
    }

    private CompletableFuture<Void> enqueueLocal(Worker home, String description, Runnable action) {
        Task task = new Task(description, action);
        Worker target = home;
        while (true) {
            synchronized (target) {
                if (!target.retiring) {
                    task.home = target;
                    pendingCount.incrementAndGet();
                    target.deque.addFirst(task);
                    signalWork();
                    return task.future;
                }
            }
            target = pickLiveWorker(null);
            if (target == null) {
                throw new RejectedExecutionException("no live workers");
            }
        }
    }

    private CompletableFuture<Void> enqueueExternal(Worker preferred, String description, Runnable action) {
        Task task = new Task(description, action);
        Worker target = preferred;
        while (true) {
            if (target == null) {
                target = pickRoundRobinWorker();
            }
            synchronized (target) {
                if (!target.retiring) {
                    task.home = target;
                    pendingCount.incrementAndGet();
                    target.deque.addLast(task);
                    signalWork();
                    return task.future;
                }
            }
            target = null;
        }
    }

    private Task stealFromOthers(Worker thief) {
        List<Worker> snapshot = new ArrayList<>(workers);
        int size = snapshot.size();
        if (size <= 1) {
            return null;
        }
        int start = ThreadLocalRandom.current().nextInt(size);
        for (int offset = 0; offset < size; offset++) {
            Worker victim = snapshot.get((start + offset) % size);
            if (victim == thief) {
                continue;
            }
            Task task = victim.deque.pollLast();
            if (task != null) {
                return task;
            }
        }
        return null;
    }

    private Worker pickRoundRobinWorker() {
        List<Worker> live = liveWorkers();
        if (live.isEmpty()) {
            throw new RejectedExecutionException("no live workers");
        }
        int index = Math.floorMod(roundRobin.getAndIncrement(), live.size());
        Worker target = live.get(index);
        if (target.retiring) {
            return pickLiveWorker(null);
        }
        return target;
    }

    private Worker pickLiveWorker(Worker exclude) {
        List<Worker> snapshot = new ArrayList<>(workers);
        if (snapshot.isEmpty()) {
            return null;
        }
        int start = ThreadLocalRandom.current().nextInt(snapshot.size());
        for (int offset = 0; offset < snapshot.size(); offset++) {
            Worker candidate = snapshot.get((start + offset) % snapshot.size());
            if (candidate != exclude && !candidate.retiring) {
                return candidate;
            }
        }
        return null;
    }

    private void executeTask(Worker executingWorker, Task task, boolean stolen) {
        Worker previousWorker = currentWorker.get();
        Task previousTask = currentTask.get();
        currentWorker.set(executingWorker);
        currentTask.set(task);
        try {
            task.action.run();
            task.future.complete(null);
        } catch (Throwable error) {
            failures.add(new TaskFailure(task.description, error));
            task.future.completeExceptionally(error);
        } finally {
            currentWorker.set(previousWorker);
            currentTask.set(previousTask);
            executedCount.incrementAndGet();
            if (stolen) {
                stolenCount.incrementAndGet();
            }
            if (pendingCount.decrementAndGet() == 0L) {
                synchronized (quiescenceMonitor) {
                    quiescenceMonitor.notifyAll();
                }
            }
        }
    }

    private void awaitWork(long backoffNanos) {
        idleLock.lock();
        try {
            workAvailable.awaitNanos(backoffNanos);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            idleLock.unlock();
        }
    }

    private void signalWork() {
        idleLock.lock();
        try {
            workAvailable.signalAll();
        } finally {
            idleLock.unlock();
        }
    }

    private List<Worker> liveWorkers() {
        List<Worker> live = new ArrayList<>();
        for (Worker worker : workers) {
            if (!worker.retiring) {
                live.add(worker);
            }
        }
        return live;
    }

    /**
     * Changes the number of live workers. Added workers start immediately;
     * removed workers finish in-flight work and transfer every queued task
     * to a surviving worker before their threads exit.
     */
    public synchronized void resize(int targetWorkers) {
        if (targetWorkers < 1) {
            throw new IllegalArgumentException("targetWorkers must be >= 1");
        }
        if (shutdown) {
            throw new IllegalStateException("scheduler is shut down");
        }
        List<Worker> live = liveWorkers();
        int current = live.size();
        if (targetWorkers > current) {
            for (int i = current; i < targetWorkers; i++) {
                startWorker();
            }
        } else if (targetWorkers < current) {
            int toRemove = current - targetWorkers;
            for (int i = 0; i < toRemove; i++) {
                Worker removed = live.get(current - 1 - i);
                synchronized (removed) {
                    removed.retiring = true;
                }
            }
            signalWork();
        }
    }

    public int getWorkerCount() {
        return liveWorkers().size();
    }

    /** Number of worker threads currently registered (including draining ones). */
    int registeredWorkerCount() {
        return workers.size();
    }

    public long getExecutedCount() {
        return executedCount.get();
    }

    public long getStolenCount() {
        return stolenCount.get();
    }

    public List<TaskFailure> getFailures() {
        return List.copyOf(failures);
    }

    /** Blocks until all currently submitted tasks have completed. */
    public boolean awaitQuiescence(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        synchronized (quiescenceMonitor) {
            while (pendingCount.get() != 0L) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(quiescenceMonitor, remaining);
            }
            return true;
        }
    }

    public void shutdown() {
        shutdown = true;
        signalWork();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (true) {
            List<Worker> snapshot = new ArrayList<>(workers);
            if (snapshot.isEmpty()) {
                return true;
            }
            for (Worker worker : snapshot) {
                Thread thread = worker.thread;
                if (thread == null) {
                    continue;
                }
                long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                if (remainingMillis <= 0L) {
                    return workers.isEmpty();
                }
                thread.join(Math.max(1L, remainingMillis));
                if (thread.isAlive() && deadline - System.nanoTime() <= 0L) {
                    return workers.isEmpty();
                }
            }
        }
    }

    @Override
    public void close() {
        shutdown();
        try {
            awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    List<Thread> workerThreads() {
        List<Thread> threads = new ArrayList<>();
        for (Worker worker : liveWorkers()) {
            threads.add(worker.thread);
        }
        return threads;
    }

    int indexOfWorkerThread(Thread thread) {
        List<Worker> live = liveWorkers();
        for (int i = 0; i < live.size(); i++) {
            if (live.get(i).thread == thread) {
                return i;
            }
        }
        return -1;
    }
}
