package com.example.gsb.scheduler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 已提交任务的外部句柄，用于等待完成、查询失败原因。
 */
public final class TaskHandle {

    private final long id;
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile Throwable error;

    Task task;

    TaskHandle(long id) {
        this.id = id;
    }

    public long id() {
        return id;
    }

    public boolean isDone() {
        return done.getCount() == 0;
    }

    /** 任务抛出的异常；正常完成时为 {@code null}。 */
    public Throwable error() {
        return error;
    }

    public void join() {
        boolean interrupted = false;
        while (true) {
            try {
                done.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return done.await(timeout, unit);
    }

    void complete(Throwable failure) {
        this.error = failure;
        done.countDown();
    }
}
