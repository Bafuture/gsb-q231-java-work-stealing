package com.example.gsb.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 固定分配调度器（对照组）：任务按轮询一次性绑定到某个工作线程的队列，
 * 线程之间<b>不</b>互相窃取。负载不均时空闲线程也只能干等。
 */
public final class FixedAssignmentScheduler implements AutoCloseable {

    private static final Runnable POISON = () -> { };

    private final List<BlockingQueue<Runnable>> queues = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();
    private final AtomicInteger roundRobin = new AtomicInteger();
    private final AtomicInteger pending = new AtomicInteger();
    private final Object quiescenceMonitor = new Object();
    private volatile boolean shutdown;

    public FixedAssignmentScheduler(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be >= 1");
        }
        for (int i = 0; i < parallelism; i++) {
            BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
            queues.add(queue);
            Thread thread = new Thread(() -> loop(queue), "fixed-worker-" + i);
            thread.setDaemon(true);
            threads.add(thread);
            thread.start();
        }
    }

    private void loop(BlockingQueue<Runnable> queue) {
        while (true) {
            Runnable task;
            try {
                task = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (task == POISON) {
                return;
            }
            try {
                task.run();
            } catch (Throwable ignored) {
                // 对照实现同样隔离异常，避免干扰计时
            } finally {
                if (pending.decrementAndGet() == 0) {
                    synchronized (quiescenceMonitor) {
                        quiescenceMonitor.notifyAll();
                    }
                }
            }
        }
    }

    public void submit(Runnable body) {
        Objects.requireNonNull(body, "body");
        if (shutdown) {
            throw new IllegalStateException("scheduler is shut down");
        }
        pending.incrementAndGet();
        int index = Math.floorMod(roundRobin.getAndIncrement(), queues.size());
        queues.get(index).add(body);
    }

    public int parallelism() {
        return threads.size();
    }

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

    @Override
    public void close() {
        shutdown = true;
        try {
            awaitQuiescence();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (BlockingQueue<Runnable> queue : queues) {
            queue.offer(POISON);
        }
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
