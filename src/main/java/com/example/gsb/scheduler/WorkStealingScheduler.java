package com.example.gsb.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * 工作窃取任务调度器。
 *
 * <ul>
 *   <li>每个工作线程持有一个 {@link WorkStealingDeque}：本线程的任务从头部 LIFO 取，
 *       空闲线程从其它队列尾部 FIFO 窃取。</li>
 *   <li>空闲时先扫描全部队列窃取，仍无任务则“让出 CPU + 指数退避 park”，绝不忙等。</li>
 *   <li>{@link #resize(int)} 支持动态增删工作线程；缩容时被移除线程的未执行任务
 *       会原子地转移到存活线程，保证不丢失。</li>
 *   <li>任务异常被隔离收集（{@link #failures()}），不影响其它任务。</li>
 *   <li>依赖任务（{@link #submitAfter} / {@link #submitAfterAll}）pin 在 home worker 上，
 *       即使前驱被窃取执行，后续任务仍回到正确线程，保持本地顺序语义。</li>
 * </ul>
 */
public final class WorkStealingScheduler implements AutoCloseable {

    private final Object resizeLock = new Object();
    private final List<Worker> workers = new ArrayList<>();
    private volatile Worker[] snapshot = new Worker[0];

    private final AtomicLong taskIds = new AtomicLong();
    private final AtomicInteger roundRobin = new AtomicInteger();
    private final AtomicInteger pending = new AtomicInteger();

    private final Object quiescenceMonitor = new Object();
    private final Queue<TaskFailure> failures = new ConcurrentLinkedQueue<>();
    private final Set<Thread> parkedThreads = ConcurrentHashMap.newKeySet();

    private final ThreadLocal<Worker> currentWorker = new ThreadLocal<>();
    private final ThreadLocal<Task> currentTask = new ThreadLocal<>();

    private volatile boolean shutdown;
    private int nextWorkerIndex;

    public WorkStealingScheduler(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be >= 1");
        }
        synchronized (resizeLock) {
            for (int i = 0; i < parallelism; i++) {
                addWorkerLocked();
            }
        }
    }

    // ------------------------------------------------------------------ 提交

    /**
     * 提交一个无依赖任务。
     *
     * <p>从工作线程内部提交时，任务进入<b>当前任务的 home worker</b> 的队列头部
     * （若当前任务是被窃取来的，则回到它原本的归属线程，而不是窃取者的队列）；
     * 从外部线程提交时按轮询分配到某个工作线程。
     */
    public TaskHandle submit(Runnable body) {
        Objects.requireNonNull(body, "body");
        Task task;
        Worker home;
        synchronized (resizeLock) {
            checkOpen();
            home = routeFromCaller();
            task = newTask(body, home, false, 0);
            pending.incrementAndGet();
            home.deque.pushHead(task);
        }
        LockSupport.unpark(home.thread);
        wakeParkedWorker();
        return task.handle;
    }

    /** 提交一个依赖单个前驱的任务；前驱完成后该任务回到前驱的 home worker 执行。 */
    public TaskHandle submitAfter(TaskHandle dependency, Runnable body) {
        return submitAfterAll(List.of(dependency), body);
    }

    /**
     * 提交一个依赖多个前驱的任务。全部前驱完成后，任务被放入其 home worker
     * （取第一个前驱的 home）的队列头部，并以 pinned 方式防止被其它线程窃取，
     * 从而保证依赖链的本地执行顺序。
     */
    public TaskHandle submitAfterAll(Collection<TaskHandle> dependencies, Runnable body) {
        Objects.requireNonNull(body, "body");
        Objects.requireNonNull(dependencies, "dependencies");
        Task task;
        boolean enqueueNow;
        Worker home;
        synchronized (resizeLock) {
            checkOpen();
            home = homeOf(dependencies);
            task = newTask(body, home, !dependencies.isEmpty(), dependencies.size());
            pending.incrementAndGet();

            int liveDeps = dependencies.size();
            for (TaskHandle handle : dependencies) {
                Task dep = handle.task;
                if (dep == null) {
                    throw new IllegalArgumentException("dependency does not belong to this scheduler");
                }
                if (!dep.addDependent(task)) {
                    // 前驱已完成：不计入未决依赖
                    liveDeps--;
                    task.dependencyFinished();
                }
            }
            enqueueNow = liveDeps == 0;
            if (enqueueNow) {
                home.deque.pushHead(task);
            }
        }
        if (enqueueNow) {
            LockSupport.unpark(home.thread);
            wakeParkedWorker();
        }
        return task.handle;
    }

    private Task newTask(Runnable body, Worker home, boolean pinned, int deps) {
        return new Task(taskIds.getAndIncrement(), body, home, pinned, deps);
    }

    private Worker homeOf(Collection<TaskHandle> dependencies) {
        if (!dependencies.isEmpty()) {
            Task first = dependencies.iterator().next().task;
            if (first != null) {
                Worker home = first.home;
                if (home != null && !home.retiring && !home.dead) {
                    return home;
                }
            }
        }
        return routeFromCaller();
    }

    /** 必须在 resizeLock 内调用。 */
    private Worker routeFromCaller() {
        Task inFlight = currentTask.get();
        if (inFlight != null) {
            Worker home = inFlight.home;
            if (home != null && !home.retiring && !home.dead) {
                return home;
            }
        }
        Worker worker = currentWorker.get();
        if (worker != null && !worker.retiring && !worker.dead) {
            return worker;
        }
        return pickActiveLocked();
    }

    /** 必须在 resizeLock 内调用。 */
    private Worker pickActiveLocked() {
        int index = Math.floorMod(roundRobin.getAndIncrement(), workers.size());
        return workers.get(index);
    }

    // -------------------------------------------------------------- 动态伸缩

    public int parallelism() {
        synchronized (resizeLock) {
            return workers.size();
        }
    }

    /**
     * 动态调整工作线程数量。
     *
     * <p>缩容时：被移除的线程先被标记为 retiring（不再接收新任务、不再窃取），
     * 其队列中剩余的任务在 {@code resizeLock} 保护下原子转移到存活线程的队列，
     * 因此任何已提交任务都不会丢失；被移除线程执行完手头任务后自行退出。
     */
    public void resize(int newParallelism) {
        if (newParallelism < 1) {
            throw new IllegalArgumentException("parallelism must be >= 1");
        }
        List<Worker> removed = new ArrayList<>();
        synchronized (resizeLock) {
            checkOpen();
            while (workers.size() < newParallelism) {
                addWorkerLocked();
            }
            while (workers.size() > newParallelism) {
                Worker victim = workers.remove(workers.size() - 1);
                victim.retiring = true;
                for (Task orphaned : victim.deque.drainAll()) {
                    Worker target = pickActiveLocked();
                    orphaned.home = target;
                    target.deque.pushHead(orphaned);
                }
                removed.add(victim);
            }
            snapshot = workers.toArray(new Worker[0]);
        }
        for (Worker worker : removed) {
            LockSupport.unpark(worker.thread);
        }
        wakeAllParked();
    }

    private void addWorkerLocked() {
        Worker worker = new Worker(this, nextWorkerIndex++);
        workers.add(worker);
        snapshot = workers.toArray(new Worker[0]);
        worker.start();
    }

    // ------------------------------------------------------------ 执行与完成

    void runTask(Task task, Worker worker) {
        currentWorker.set(worker);
        currentTask.set(task);
        Throwable failure = null;
        try {
            task.body.run();
        } catch (Throwable t) {
            failure = t;
            failures.add(new TaskFailure(task.id, worker.thread.getName(), t));
        } finally {
            try {
                task.handle.complete(failure);
                for (Task dependent : task.completionSnapshot()) {
                    if (dependent.dependencyFinished()) {
                        enqueueReady(dependent);
                    }
                }
            } finally {
                currentTask.remove();
                currentWorker.remove();
                if (pending.decrementAndGet() == 0) {
                    synchronized (quiescenceMonitor) {
                        quiescenceMonitor.notifyAll();
                    }
                }
            }
        }
    }

    private void enqueueReady(Task task) {
        synchronized (resizeLock) {
            Worker home = task.home;
            if (home == null || home.retiring || home.dead) {
                home = pickActiveLocked();
                task.home = home;
            }
            home.deque.pushHead(task);
        }
        wakeParkedWorker();
    }

    // ---------------------------------------------------------- 空闲与唤醒

    Worker[] workerSnapshot() {
        return snapshot;
    }

    void markParked(Thread thread) {
        parkedThreads.add(thread);
    }

    void clearParked(Thread thread) {
        parkedThreads.remove(thread);
    }

    private void wakeParkedWorker() {
        for (Thread thread : parkedThreads) {
            LockSupport.unpark(thread);
            break;
        }
    }

    private void wakeAllParked() {
        for (Thread thread : parkedThreads) {
            LockSupport.unpark(thread);
        }
    }

    // ---------------------------------------------------------- 状态与关闭

    public List<TaskFailure> failures() {
        return List.copyOf(failures);
    }

    boolean isShutdown() {
        return shutdown;
    }

    boolean isQuiescent() {
        return pending.get() == 0;
    }

    /** 阻塞直到所有已提交任务（含尚未满足依赖的任务）执行完毕。 */
    public void awaitQuiescence() throws InterruptedException {
        synchronized (quiescenceMonitor) {
            while (pending.get() != 0) {
                quiescenceMonitor.wait();
            }
        }
    }

    public boolean awaitQuiescence(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        synchronized (quiescenceMonitor) {
            long remaining;
            while (pending.get() != 0 && (remaining = deadline - System.nanoTime()) > 0) {
                TimeUnit.NANOSECONDS.timedWait(quiescenceMonitor, remaining);
            }
            return pending.get() == 0;
        }
    }

    /**
     * 停止接收新任务；已提交任务继续执行，工作线程在全部任务完成后退出。
     */
    public void shutdown() {
        synchronized (resizeLock) {
            if (shutdown) {
                return;
            }
            shutdown = true;
        }
        wakeAllParked();
    }

    public void awaitTermination() throws InterruptedException {
        List<Worker> all;
        synchronized (resizeLock) {
            all = new ArrayList<>(workers);
        }
        for (Worker worker : all) {
            worker.thread.join();
        }
    }

    @Override
    public void close() {
        shutdown();
        try {
            awaitTermination();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void checkOpen() {
        if (shutdown) {
            throw new IllegalStateException("scheduler is shut down");
        }
    }
}
