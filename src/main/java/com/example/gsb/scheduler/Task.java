package com.example.gsb.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 调度器内部的任务表示。
 *
 * <p>{@code home} 是任务的归属工作线程（本地顺序语义的“正确线程”）。任务即使被其它线程窃取执行，
 * 它在执行过程中提交的后续任务、以及依赖它的后续任务，仍然回到 {@code home} 的队列。
 *
 * <p>{@code pinned} 的任务只能被 home worker 取走，窃取者会跳过它，用于显式依赖链
 * （{@link WorkStealingScheduler#submitAfter}）以保证链上顺序不被窃取破坏。
 */
final class Task {

    final long id;
    final Runnable body;
    final boolean pinned;
    final TaskHandle handle;

    volatile Worker home;

    private final AtomicInteger pendingDeps;
    private final List<Task> dependents = new ArrayList<>();
    private boolean completed;

    Task(long id, Runnable body, Worker home, boolean pinned, int pendingDeps) {
        this.id = id;
        this.body = body;
        this.home = home;
        this.pinned = pinned;
        this.pendingDeps = new AtomicInteger(pendingDeps);
        this.handle = new TaskHandle(id);
        this.handle.task = this;
    }

    /**
     * 注册一个依赖本任务的后续任务。
     *
     * @return {@code false} 表示本任务已经完成，调用方不应再计入未决依赖
     */
    boolean addDependent(Task dependent) {
        synchronized (this) {
            if (completed) {
                return false;
            }
            dependents.add(dependent);
            return true;
        }
    }

    /**
     * 任务执行完毕后调用，原子地取出全部后续任务（保证只通知一次）。
     */
    List<Task> completionSnapshot() {
        synchronized (this) {
            completed = true;
            return new ArrayList<>(dependents);
        }
    }

    /**
     * 一个未决依赖完成；返回 {@code true} 时该任务的全部依赖均已完成，可以入队。
     */
    boolean dependencyFinished() {
        return pendingDeps.decrementAndGet() == 0;
    }
}
