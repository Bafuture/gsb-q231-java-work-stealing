package com.example.gsb.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DynamicResizeTest {

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void scaleUpAndDownDuringExecutionWithoutLosingOrDuplicatingTasks() throws Exception {
        int total = 600;
        AtomicInteger[] counters = new AtomicInteger[total];
        for (int i = 0; i < total; i++) {
            counters[i] = new AtomicInteger();
        }

        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(2)) {
            assertThat(scheduler.parallelism()).isEqualTo(2);

            int submitted = 0;
            for (int batch = 0; batch < 3; batch++) {
                for (int k = 0; k < total / 3; k++) {
                    int index = submitted++;
                    scheduler.submit(() -> {
                        counters[index].incrementAndGet();
                        sleepQuietly(1);
                    });
                }
                if (batch == 0) {
                    scheduler.resize(6);
                    assertThat(scheduler.parallelism()).isEqualTo(6);
                } else if (batch == 1) {
                    scheduler.resize(1);
                    assertThat(scheduler.parallelism()).isEqualTo(1);
                }
            }

            assertThat(scheduler.awaitQuiescence(60, TimeUnit.SECONDS)).isTrue();
        }

        for (int i = 0; i < total; i++) {
            assertThat(counters[i]).as("task %d must run exactly once", i).hasValue(1);
        }
    }

    @Test
    void removingWorkersRedistributesTheirQueuedTasks() throws Exception {
        int queued = 200;
        AtomicInteger runCount = new AtomicInteger();
        CountDownLatch allBlockersRunning = new CountDownLatch(4);
        CountDownLatch release = new CountDownLatch(1);

        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(4)) {
            // 占满全部 4 个 worker
            for (int i = 0; i < 4; i++) {
                scheduler.submit(() -> {
                    allBlockersRunning.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            assertThat(allBlockersRunning.await(5, TimeUnit.SECONDS)).isTrue();

            // 这 200 个任务均匀堆在 4 个队列里，worker 都腾不出手
            for (int i = 0; i < queued; i++) {
                scheduler.submit(runCount::incrementAndGet);
            }

            // 缩到 1 个 worker：3 个被移除线程的未执行任务必须转移到存活线程
            scheduler.resize(1);
            assertThat(scheduler.parallelism()).isEqualTo(1);

            release.countDown();
            assertThat(scheduler.awaitQuiescence(30, TimeUnit.SECONDS)).isTrue();
            assertThat(runCount).hasValue(queued);
        }
    }

    @Test
    void scaleUpIncreasesThroughputOfBackloggedQueue() throws Exception {
        int tasks = 120;
        AtomicInteger runCount = new AtomicInteger();
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(1)) {
            scheduler.submit(() -> {
                blockerStarted.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(blockerStarted.await(5, TimeUnit.SECONDS)).isTrue();

            for (int i = 0; i < tasks; i++) {
                scheduler.submit(() -> {
                    runCount.incrementAndGet();
                    sleepQuietly(5);
                });
            }

            // 积压形成后扩容到 6 个 worker，再放行
            scheduler.resize(6);
            assertThat(scheduler.parallelism()).isEqualTo(6);
            release.countDown();

            assertThat(scheduler.awaitQuiescence(30, TimeUnit.SECONDS)).isTrue();
            assertThat(runCount).hasValue(tasks);
        }
    }
}
