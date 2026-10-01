package com.example.gsb.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * 负载不均场景：重任务在轮询分配下全部落到同一个 worker 的队列。
 * 固定分配：该 worker 串行执行全部重任务；工作窃取：空闲 worker 从尾部偷走重任务并行执行。
 */
class LoadComparisonTest {

    private static final int WORKERS = 4;
    private static final int HEAVY_COUNT = 4;
    private static final int HEAVY_MILLIS = 250;
    private static final int LIGHT_COUNT = 24;
    private static final int LIGHT_MILLIS = 5;

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** 构造提交序列：下标 i % WORKERS == 0 的位置放重任务，使固定分配下重任务全部进入 worker0。 */
    private static List<Runnable> skewedWorkload(AtomicInteger executed) {
        List<Runnable> tasks = new ArrayList<>();
        int heavies = 0;
        int lights = 0;
        int index = 0;
        while (heavies < HEAVY_COUNT || lights < LIGHT_COUNT) {
            if (index % WORKERS == 0 && heavies < HEAVY_COUNT) {
                heavies++;
                tasks.add(() -> {
                    sleepQuietly(HEAVY_MILLIS);
                    executed.incrementAndGet();
                });
            } else if (lights < LIGHT_COUNT) {
                lights++;
                tasks.add(() -> {
                    sleepQuietly(LIGHT_MILLIS);
                    executed.incrementAndGet();
                });
            } else {
                tasks.add(executed::incrementAndGet);
            }
            index++;
        }
        return tasks;
    }

    private static long runFixed(List<Runnable> tasks) throws InterruptedException {
        try (FixedAssignmentScheduler scheduler = new FixedAssignmentScheduler(WORKERS)) {
            long start = System.nanoTime();
            for (Runnable task : tasks) {
                scheduler.submit(task);
            }
            assertThat(scheduler.awaitQuiescence(60, TimeUnit.SECONDS)).isTrue();
            return System.nanoTime() - start;
        }
    }

    private static long runStealing(List<Runnable> tasks) throws InterruptedException {
        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(WORKERS)) {
            long start = System.nanoTime();
            for (Runnable task : tasks) {
                scheduler.submit(task);
            }
            assertThat(scheduler.awaitQuiescence(60, TimeUnit.SECONDS)).isTrue();
            return System.nanoTime() - start;
        }
    }

    @Test
    void workStealingOutperformsFixedAssignmentUnderSkewedLoad() throws Exception {
        // 预热，消除 JIT 影响
        AtomicInteger warmup = new AtomicInteger();
        runFixed(skewedWorkload(warmup));
        runStealing(skewedWorkload(warmup));

        AtomicInteger fixedExecuted = new AtomicInteger();
        long fixedNanos = runFixed(skewedWorkload(fixedExecuted));

        AtomicInteger stealingExecuted = new AtomicInteger();
        long stealingNanos = runStealing(skewedWorkload(stealingExecuted));

        long fixedMillis = fixedNanos / 1_000_000;
        long stealingMillis = stealingNanos / 1_000_000;
        int totalTasks = HEAVY_COUNT + LIGHT_COUNT;

        System.out.printf(
                "[负载不均对比] workers=%d, 重任务=%dx%dms, 轻任务=%dx%dms%n"
                        + "  固定分配完成时间: %d ms (执行 %d 个任务)%n"
                        + "  工作窃取完成时间: %d ms (执行 %d 个任务)%n"
                        + "  加速比: %.2fx%n",
                WORKERS, HEAVY_COUNT, HEAVY_MILLIS, LIGHT_COUNT, LIGHT_MILLIS,
                fixedMillis, fixedExecuted.get(),
                stealingMillis, stealingExecuted.get(),
                (double) fixedNanos / stealingNanos);

        assertThat(fixedExecuted).hasValue(totalTasks);
        assertThat(stealingExecuted).hasValue(totalTasks);

        // 固定分配下 worker0 串行跑 4 个重任务 ≈ 1000ms；
        // 工作窃取下 4 个重任务并行 ≈ 250ms + 轻任务开销。留出充足余量。
        assertThat(stealingNanos)
                .as("work stealing must be significantly faster than fixed assignment")
                .isLessThan((long) (fixedNanos * 0.65));
    }
}
