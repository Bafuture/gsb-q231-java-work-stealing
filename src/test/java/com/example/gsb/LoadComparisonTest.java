package com.example.gsb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Compares the work-stealing scheduler against a fixed round-robin
 * assignment pool under a deliberately imbalanced workload: every round
 * submits one heavy task followed by light tasks, so fixed assignment
 * piles all heavy tasks onto a single worker while work stealing spreads
 * them across idle workers.
 */
class LoadComparisonTest {

    private static final int WORKERS = 4;
    private static final int ROUNDS = 25;
    private static final long HEAVY_MILLIS = 25;
    private static final long LIGHT_MILLIS = 1;

    @Test
    void workStealingBeatsFixedAssignmentUnderImbalancedLoad() throws Exception {
        long fixedNanos = runFixedAssignment();
        long stealingNanos = runWorkStealing();

        double speedup = (double) fixedNanos / stealingNanos;
        System.out.printf(
                "[load-comparison] workers=%d, rounds=%d, heavy=%dms, light=%dms%n",
                WORKERS, ROUNDS, HEAVY_MILLIS, LIGHT_MILLIS);
        System.out.printf(
                "[load-comparison] fixed-assignment: %d ms, work-stealing: %d ms, speedup: %.2fx%n",
                TimeUnit.NANOSECONDS.toMillis(fixedNanos),
                TimeUnit.NANOSECONDS.toMillis(stealingNanos),
                speedup);

        assertThat(stealingNanos).isLessThan(fixedNanos);
    }

    private static long runWorkStealing() throws Exception {
        AtomicInteger executed = new AtomicInteger();
        long stolen;
        long elapsed;
        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(WORKERS)) {
            CountDownLatch done = new CountDownLatch(ROUNDS * WORKERS);
            long start = System.nanoTime();
            submitWorkload(scheduler::submit, done, executed);
            assertThat(done.await(120, TimeUnit.SECONDS)).isTrue();
            elapsed = System.nanoTime() - start;
            stolen = scheduler.getStolenCount();
        }
        assertThat(executed.get()).isEqualTo(ROUNDS * WORKERS);
        assertThat(stolen).as("idle workers must steal the heavy tasks").isGreaterThan(0);
        System.out.printf("[load-comparison] work-stealing stole %d tasks%n", stolen);
        return elapsed;
    }

    private static long runFixedAssignment() throws Exception {
        AtomicInteger executed = new AtomicInteger();
        long elapsed;
        try (FixedAssignmentPool pool = new FixedAssignmentPool(WORKERS)) {
            CountDownLatch done = new CountDownLatch(ROUNDS * WORKERS);
            long start = System.nanoTime();
            submitWorkload(pool::submit, done, executed);
            assertThat(done.await(120, TimeUnit.SECONDS)).isTrue();
            elapsed = System.nanoTime() - start;
        }
        assertThat(executed.get()).isEqualTo(ROUNDS * WORKERS);
        return elapsed;
    }

    private static void submitWorkload(TaskSubmitter submitter, CountDownLatch done, AtomicInteger executed) {
        for (int round = 0; round < ROUNDS; round++) {
            for (int slot = 0; slot < WORKERS; slot++) {
                boolean heavy = slot == 0;
                submitter.submit(() -> {
                    sleepUninterruptibly(heavy ? HEAVY_MILLIS : LIGHT_MILLIS);
                    executed.incrementAndGet();
                    done.countDown();
                });
            }
        }
    }

    private static void sleepUninterruptibly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @FunctionalInterface
    private interface TaskSubmitter {
        void submit(Runnable task);
    }

    /** Baseline: fixed round-robin assignment, one blocking queue per thread, no stealing. */
    private static final class FixedAssignmentPool implements AutoCloseable {
        private final List<LinkedBlockingQueue<Runnable>> queues = new ArrayList<>();
        private final List<Thread> threads = new ArrayList<>();
        private final AtomicInteger cursor = new AtomicInteger();
        private volatile boolean closed;

        FixedAssignmentPool(int workers) {
            for (int i = 0; i < workers; i++) {
                LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
                queues.add(queue);
                Thread thread = new Thread(() -> runLoop(queue), "fixed-worker-" + i);
                thread.setDaemon(true);
                threads.add(thread);
                thread.start();
            }
        }

        private void runLoop(LinkedBlockingQueue<Runnable> queue) {
            try {
                while (!closed || !queue.isEmpty()) {
                    Runnable task = queue.poll(10, TimeUnit.MILLISECONDS);
                    if (task != null) {
                        task.run();
                    }
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        void submit(Runnable task) {
            int index = Math.floorMod(cursor.getAndIncrement(), queues.size());
            queues.get(index).add(task);
        }

        @Override
        public void close() throws InterruptedException {
            closed = true;
            for (Thread thread : threads) {
                thread.join(30_000);
            }
        }
    }
}
