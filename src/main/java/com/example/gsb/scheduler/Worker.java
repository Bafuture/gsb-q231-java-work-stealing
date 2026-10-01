package com.example.gsb.scheduler;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

/**
 * 一个工作线程：维护自己的 {@link WorkStealingDeque}，并运行
 * “本地取头部 → 随机起点扫描窃取尾部 → 退让等待”的调度循环。
 */
final class Worker implements Runnable {

    private static final int SPIN_YIELDS = 3;
    private static final long INITIAL_PARK_NANOS = 1_000L;
    private static final long MAX_PARK_NANOS = 1_000_000L;

    final WorkStealingScheduler scheduler;
    final WorkStealingDeque deque = new WorkStealingDeque();
    final Thread thread;

    /** 缩容中：不再窃取，本地队列清空后退出。 */
    volatile boolean retiring;
    /** 线程已经退出。 */
    volatile boolean dead;

    Worker(WorkStealingScheduler scheduler, int index) {
        this.scheduler = scheduler;
        this.thread = new Thread(this, "wss-worker-" + index);
        this.thread.setDaemon(true);
    }

    void start() {
        thread.start();
    }

    @Override
    public void run() {
        long parkNanos = INITIAL_PARK_NANOS;
        int idleRounds = 0;

        while (true) {
            Task task = deque.popHead();
            if (task == null && !retiring) {
                task = stealFromOthers();
            }

            if (task != null) {
                idleRounds = 0;
                parkNanos = INITIAL_PARK_NANOS;
                scheduler.runTask(task, this);
                continue;
            }

            if (retiring && deque.isEmpty()) {
                dead = true;
                return;
            }
            if (scheduler.isShutdown() && scheduler.isQuiescent()) {
                dead = true;
                return;
            }

            // 退让策略：先让出少量 CPU 时间片，再指数退避地 park（绝不忙等）。
            idleRounds++;
            if (idleRounds <= SPIN_YIELDS) {
                Thread.yield();
            } else {
                scheduler.markParked(thread);
                LockSupport.parkNanos(parkNanos);
                scheduler.clearParked(thread);
                parkNanos = Math.min(parkNanos * 2, MAX_PARK_NANOS);
            }
        }
    }

    private Task stealFromOthers() {
        Worker[] peers = scheduler.workerSnapshot();
        if (peers.length <= 1) {
            return null;
        }
        int start = ThreadLocalRandom.current().nextInt(peers.length);
        for (int i = 0; i < peers.length; i++) {
            Worker victim = peers[(start + i) % peers.length];
            if (victim == this) {
                continue;
            }
            Task stolen = victim.deque.stealTail();
            if (stolen != null) {
                return stolen;
            }
        }
        return null;
    }
}
