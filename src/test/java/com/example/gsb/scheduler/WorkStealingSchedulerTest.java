package com.example.gsb.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class WorkStealingSchedulerTest {

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void idleWorkersStealFromBusyWorkerAndEveryTaskRunsExactlyOnce() throws Exception {
        int followUps = 400;
        AtomicInteger[] counters = new AtomicInteger[followUps];
        for (int i = 0; i < followUps; i++) {
            counters[i] = new AtomicInteger();
        }
        Set<String> participatingWorkers = ConcurrentHashMap.newKeySet();

        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(4)) {
            // 由一个“生产者”任务在单个 worker 的本地队列中制造大量任务，
            // 其余空闲 worker 只能从它的尾部窃取。
            scheduler.submit(() -> {
                for (int i = 0; i < followUps; i++) {
                    int index = i;
                    scheduler.submit(() -> {
                        counters[index].incrementAndGet();
                        participatingWorkers.add(Thread.currentThread().getName());
                        sleepQuietly(1);
                    });
                }
            });

            assertThat(scheduler.awaitQuiescence(30, TimeUnit.SECONDS)).isTrue();
        }

        for (int i = 0; i < followUps; i++) {
            assertThat(counters[i]).as("task %d must run exactly once", i).hasValue(1);
        }
        assertThat(participatingWorkers.size())
                .as("work stealing must engage more than one worker")
                .isGreaterThanOrEqualTo(2);
    }

    @Test
    void randomTaskGraphRunsEveryTaskExactlyOnceAndRespectsDependencyOrder() throws Exception {
        int taskCount = 2_000;
        AtomicInteger[] counters = new AtomicInteger[taskCount];
        long[] completionOrder = new long[taskCount];
        for (int i = 0; i < taskCount; i++) {
            counters[i] = new AtomicInteger();
        }
        AtomicLong sequence = new AtomicLong();
        List<int[]> edges = new ArrayList<>();

        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(4)) {
            List<TaskHandle> handles = new ArrayList<>();
            Random random = new Random(42);
            for (int i = 0; i < taskCount; i++) {
                int index = i;
                Runnable body = () -> {
                    counters[index].incrementAndGet();
                    completionOrder[index] = sequence.getAndIncrement();
                };
                if (i == 0) {
                    handles.add(scheduler.submit(body));
                    continue;
                }
                int depCount = 1 + random.nextInt(3);
                List<TaskHandle> deps = new ArrayList<>();
                for (int k = 0; k < depCount; k++) {
                    int dep = random.nextInt(i);
                    deps.add(handles.get(dep));
                    edges.add(new int[] {dep, i});
                }
                handles.add(scheduler.submitAfterAll(deps, body));
            }

            assertThat(scheduler.awaitQuiescence(60, TimeUnit.SECONDS)).isTrue();
        }

        for (int i = 0; i < taskCount; i++) {
            assertThat(counters[i]).as("task %d must run exactly once", i).hasValue(1);
        }
        for (int[] edge : edges) {
            assertThat(completionOrder[edge[1]])
                    .as("task %d must run after its dependency %d", edge[1], edge[0])
                    .isGreaterThan(completionOrder[edge[0]]);
        }
    }

    @Test
    void failingTasksAreIsolatedAndCollected() throws Exception {
        int failing = 10;
        int good = 90;
        AtomicInteger goodRuns = new AtomicInteger();
        List<TaskHandle> failingHandles = new ArrayList<>();
        List<TaskHandle> goodHandles = new ArrayList<>();

        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(3)) {
            for (int i = 0; i < failing; i++) {
                int index = i;
                failingHandles.add(scheduler.submit(() -> {
                    throw new IllegalStateException("boom-" + index);
                }));
            }
            for (int i = 0; i < good; i++) {
                goodHandles.add(scheduler.submit(goodRuns::incrementAndGet));
            }

            assertThat(scheduler.awaitQuiescence(30, TimeUnit.SECONDS)).isTrue();

            assertThat(goodRuns).hasValue(good);
            assertThat(scheduler.failures()).hasSize(failing);
            assertThat(scheduler.failures())
                    .allSatisfy(f -> assertThat(f.cause()).isInstanceOf(IllegalStateException.class));
            for (TaskHandle handle : failingHandles) {
                assertThat(handle.isDone()).isTrue();
                assertThat(handle.error()).isInstanceOf(IllegalStateException.class);
            }
            for (TaskHandle handle : goodHandles) {
                assertThat(handle.error()).isNull();
            }
        }
    }

    @Test
    void dependentTasksReturnToHomeWorkerAfterPredecessorWasStolen() throws Exception {
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());
        String[] threads = new String[3];

        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(2)) {
            // 通过已完成的 home 任务把阻塞任务 pin 到指定 worker，杜绝被对端窃取
            TaskHandle home0 = scheduler.submit(() -> { });   // -> worker0
            TaskHandle home1 = scheduler.submit(() -> { });   // -> worker1
            home0.join();
            home1.join();

            // pinned 到 worker0 的长阻塞，占住 worker0
            scheduler.submitAfter(home0, () -> {
                blockerStarted.countDown();
                try {
                    releaseBlocker.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            // pinned 到 worker1 的短阻塞，保证链头入队时 worker1 暂时没空
            scheduler.submitAfter(home1, () -> sleepQuietly(150));

            assertThat(blockerStarted.await(5, TimeUnit.SECONDS)).isTrue();

            // 链头提交到 worker0 的队列；worker0 被占住，只能被 worker1 窃取
            TaskHandle a = scheduler.submit(() -> {
                threads[0] = Thread.currentThread().getName();
                executionOrder.add("A");
            });
            TaskHandle b = scheduler.submitAfter(a, () -> {
                threads[1] = Thread.currentThread().getName();
                executionOrder.add("B");
            });
            TaskHandle c = scheduler.submitAfter(b, () -> {
                threads[2] = Thread.currentThread().getName();
                executionOrder.add("C");
            });

            a.join();
            assertThat(threads[0])
                    .as("A must be stolen by the idle worker1 while worker0 is blocked")
                    .isEqualTo("wss-worker-1");

            releaseBlocker.countDown();
            c.join();
            assertThat(scheduler.awaitQuiescence(10, TimeUnit.SECONDS)).isTrue();

            // B、C 是依赖任务，pinned 到 home（worker0），不会被 worker1 再次窃取
            assertThat(threads[1]).isEqualTo("wss-worker-0");
            assertThat(threads[2]).isEqualTo("wss-worker-0");
            assertThat(executionOrder).containsExactly("A", "B", "C");
        }
    }

    @Test
    void dependencyChainPreservesLocalOrderOnSingleWorker() throws Exception {
        int chainLength = 200;
        List<Integer> order = Collections.synchronizedList(new ArrayList<>());

        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(1)) {
            TaskHandle head = scheduler.submit(() -> order.add(0));
            TaskHandle current = head;
            for (int i = 1; i < chainLength; i++) {
                int value = i;
                current = scheduler.submitAfter(current, () -> order.add(value));
            }
            current.join();
            assertThat(scheduler.awaitQuiescence(10, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(order).hasSize(chainLength);
        for (int i = 0; i < chainLength; i++) {
            assertThat(order.get(i)).isEqualTo(i);
        }
    }

    @Test
    void idleSchedulerDoesNotBusyWait() throws Exception {
        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(4)) {
            // 让所有 worker 进入退避 park 状态
            Thread.sleep(200);

            long cpuBefore = totalWorkerCpuTime();
            Thread.sleep(500);
            long cpuAfter = totalWorkerCpuTime();

            long cpuMillis = (cpuAfter - cpuBefore) / 1_000_000;
            assertThat(cpuMillis)
                    .as("idle workers must not burn CPU (measured %d ms in 500 ms window)", cpuMillis)
                    .isLessThan(100);
        }
    }

    private static long totalWorkerCpuTime() {
        long total = 0;
        java.lang.management.ThreadMXBean bean = java.lang.management.ManagementFactory.getThreadMXBean();
        if (bean.isThreadCpuTimeSupported()) {
            bean.setThreadCpuTimeEnabled(true);
        }
        for (long id : bean.getAllThreadIds()) {
            java.lang.management.ThreadInfo info = bean.getThreadInfo(id);
            if (info != null && info.getThreadName().startsWith("wss-worker-")) {
                long cpu = bean.getThreadCpuTime(id);
                if (cpu > 0) {
                    total += cpu;
                }
            }
        }
        return total;
    }
}
