package com.example.gsb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class WorkStealingSchedulerTest {

    private static void awaitLatch(CountDownLatch latch) throws InterruptedException {
        assertThat(latch.await(20, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void ownerPopsHeadAndIdleWorkerStealsFromTail() throws Exception {
        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(2)) {
            CountDownLatch blockerStarted = new CountDownLatch(1);
            CountDownLatch releaseBlocker = new CountDownLatch(1);
            AtomicReference<Thread> blockerThread = new AtomicReference<>();

            scheduler.submit("blocker", () -> {
                blockerThread.set(Thread.currentThread());
                blockerStarted.countDown();
                try {
                    releaseBlocker.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            });
            awaitLatch(blockerStarted);

            int busyIndex = scheduler.indexOfWorkerThread(blockerThread.get());
            int idleIndex = 1 - busyIndex;

            int taskCount = 50;
            CountDownLatch stolenTasksDone = new CountDownLatch(taskCount);
            AtomicReference<Thread> firstStealingThread = new AtomicReference<>();
            for (int i = 0; i < taskCount; i++) {
                scheduler.submitOn(busyIndex, () -> {
                    firstStealingThread.compareAndSet(null, Thread.currentThread());
                    stolenTasksDone.countDown();
                });
            }

            assertThat(stolenTasksDone.await(20, TimeUnit.SECONDS)).isTrue();
            Thread stealer = scheduler.workerThreads().get(idleIndex);
            assertThat(firstStealingThread.get()).isEqualTo(stealer);
            assertThat(scheduler.getStolenCount()).isGreaterThan(0);

            releaseBlocker.countDown();
        }
    }

    @Test
    void everyTaskInARandomTaskGraphRunsExactlyOnce() throws Exception {
        int workers = 4;
        int maxTasks = 5_000;
        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(workers)) {
            AtomicInteger tickets = new AtomicInteger();
            AtomicInteger submitted = new AtomicInteger();
            AtomicInteger executed = new AtomicInteger();
            AtomicIntegerArray perTaskExecutions = new AtomicIntegerArray(maxTasks);

            class Node implements Runnable {
                private final int index;

                Node(int index) {
                    this.index = index;
                }

                @Override
                public void run() {
                    perTaskExecutions.incrementAndGet(index);
                    executed.incrementAndGet();
                    int children = ThreadLocalRandom.current().nextInt(4);
                    for (int i = 0; i < children; i++) {
                        int childIndex = tickets.getAndIncrement();
                        if (childIndex < maxTasks) {
                            submitted.incrementAndGet();
                            scheduler.submit(new Node(childIndex));
                        }
                    }
                }
            }

            for (int i = 0; i < workers * 2; i++) {
                int index = tickets.getAndIncrement();
                if (index < maxTasks) {
                    submitted.incrementAndGet();
                    scheduler.submit(new Node(index));
                }
            }

            assertThat(scheduler.awaitQuiescence(60, TimeUnit.SECONDS)).isTrue();

            assertThat(executed.get()).isEqualTo(submitted.get());
            assertThat(scheduler.getExecutedCount()).isEqualTo(submitted.get());
            for (int i = 0; i < submitted.get(); i++) {
                assertThat(perTaskExecutions.get(i))
                        .as("task %d must execute exactly once", i)
                        .isEqualTo(1);
            }
        }
    }

    @Test
    void workerCountCanBeIncreasedAndDecreased() throws Exception {
        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(2)) {
            AtomicInteger executed = new AtomicInteger();
            Runnable shortTask = () -> {
                try {
                    Thread.sleep(2);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
                executed.incrementAndGet();
            };

            for (int i = 0; i < 100; i++) {
                scheduler.submit(shortTask);
            }
            scheduler.resize(6);
            assertThat(scheduler.getWorkerCount()).isEqualTo(6);
            assertThat(scheduler.awaitQuiescence(30, TimeUnit.SECONDS)).isTrue();
            assertThat(executed.get()).isEqualTo(100);

            scheduler.resize(1);
            assertThat(scheduler.getWorkerCount()).isEqualTo(1);
            for (int i = 0; i < 50; i++) {
                scheduler.submit(shortTask);
            }
            assertThat(scheduler.awaitQuiescence(30, TimeUnit.SECONDS)).isTrue();
            assertThat(executed.get()).isEqualTo(150);

            waitUntil(() -> scheduler.registeredWorkerCount() == 1, 20_000);
            assertThat(scheduler.registeredWorkerCount()).isEqualTo(1);
        }
    }

    @Test
    void pendingTasksOfRemovedWorkerAreNotLost() throws Exception {
        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(4)) {
            int taskCount = 300;
            AtomicInteger executed = new AtomicInteger();
            CountDownLatch done = new CountDownLatch(taskCount);
            for (int i = 0; i < taskCount; i++) {
                scheduler.submitOn(3, () -> {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                    executed.incrementAndGet();
                    done.countDown();
                });
            }

            scheduler.resize(1);
            assertThat(scheduler.getWorkerCount()).isEqualTo(1);
            assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
            assertThat(executed.get()).isEqualTo(taskCount);
            assertThat(scheduler.getExecutedCount()).isEqualTo(taskCount);
        }
    }

    @Test
    void aFailingTaskDoesNotAffectOthersAndFailuresAreCollected() throws Exception {
        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(2)) {
            int goodTasks = 40;
            int badTasks = 10;
            AtomicInteger goodExecuted = new AtomicInteger();
            List<CompletableFuture<Void>> badFutures = new ArrayList<>();
            List<CompletableFuture<Void>> goodFutures = new ArrayList<>();

            for (int i = 0; i < goodTasks + badTasks; i++) {
                if (i % 5 == 0) {
                    int id = i;
                    badFutures.add(scheduler.submit("bad-" + id, () -> {
                        throw new IllegalStateException("boom-" + id);
                    }));
                } else {
                    goodFutures.add(scheduler.submit("good-" + i, () -> {
                        goodExecuted.incrementAndGet();
                    }));
                }
            }

            assertThat(scheduler.awaitQuiescence(30, TimeUnit.SECONDS)).isTrue();
            assertThat(goodExecuted.get()).isEqualTo(goodTasks);
            assertThat(scheduler.getExecutedCount()).isEqualTo(goodTasks + badTasks);
            assertThat(scheduler.getFailures()).hasSize(badTasks);
            assertThat(scheduler.getFailures())
                    .allSatisfy(failure -> assertThat(failure.error()).isInstanceOf(IllegalStateException.class))
                    .allSatisfy(failure -> assertThat(failure.error().getMessage()).startsWith("boom-"));
            badFutures.forEach(future -> assertThat(future).isCompletedExceptionally());
            goodFutures.forEach(future -> assertThat(future).isCompleted());

            CountDownLatch afterFailures = new CountDownLatch(1);
            scheduler.submit(afterFailures::countDown);
            assertThat(afterFailures.await(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void continuationOfAStealenTaskReturnsToItsHomeWorker() throws Exception {
        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(2)) {
            CountDownLatch blocker1Started = new CountDownLatch(1);
            CountDownLatch blocker2Started = new CountDownLatch(1);
            CountDownLatch releaseBlocker1 = new CountDownLatch(1);
            CountDownLatch releaseBlocker2 = new CountDownLatch(1);
            CountDownLatch parentRunning = new CountDownLatch(1);
            CountDownLatch releaseParent = new CountDownLatch(1);
            CountDownLatch childDone = new CountDownLatch(1);

            AtomicReference<Thread> homeThread = new AtomicReference<>();
            AtomicReference<Thread> parentThread = new AtomicReference<>();
            AtomicReference<Thread> childThread = new AtomicReference<>();

            Runnable blocker1 = () -> {
                homeThread.set(Thread.currentThread());
                blocker1Started.countDown();
                try {
                    releaseBlocker1.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            };
            Runnable blocker2 = () -> {
                blocker2Started.countDown();
                try {
                    releaseBlocker2.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            };

            scheduler.submit("blocker1", blocker1);
            awaitLatch(blocker1Started);
            int busyIndex = scheduler.indexOfWorkerThread(homeThread.get());
            int otherIndex = 1 - busyIndex;
            scheduler.submitOn(otherIndex, "blocker2", blocker2);
            awaitLatch(blocker2Started);

            scheduler.submitOn(busyIndex, "parent", () -> {
                parentThread.set(Thread.currentThread());
                scheduler.submit("child", () -> {
                    childThread.set(Thread.currentThread());
                    childDone.countDown();
                });
                parentRunning.countDown();
                try {
                    releaseParent.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            });

            releaseBlocker2.countDown();
            awaitLatch(parentRunning);

            assertThat(parentThread.get()).isNotEqualTo(homeThread.get());

            releaseBlocker1.countDown();
            assertThat(childDone.await(20, TimeUnit.SECONDS)).isTrue();
            assertThat(childThread.get()).isEqualTo(homeThread.get());

            releaseParent.countDown();
            assertThat(scheduler.awaitQuiescence(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void locallyForkedTasksKeepLifoOrderOnTheHomeWorker() throws Exception {
        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(1)) {
            List<String> order = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch done = new CountDownLatch(1);

            scheduler.submit("parent", () -> {
                order.add("parent");
                scheduler.submit("c1", () -> order.add("c1"));
                scheduler.submit("c2", () -> order.add("c2"));
                scheduler.submit("c3", () -> {
                    order.add("c3");
                    done.countDown();
                });
            });

            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(scheduler.awaitQuiescence(10, TimeUnit.SECONDS)).isTrue();
            assertThat(order).containsExactly("parent", "c3", "c2", "c1");
        }
    }

    @Test
    void idleWorkersParkInsteadOfBusySpinning() throws Exception {
        try (WorkStealingScheduler scheduler = new WorkStealingScheduler(3)) {
            Thread.sleep(300);
            List<Thread> threads = scheduler.workerThreads();
            int parked = 0;
            int samples = 0;
            for (int i = 0; i < 40; i++) {
                for (Thread thread : threads) {
                    samples++;
                    Thread.State state = thread.getState();
                    if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
                        parked++;
                    }
                }
                Thread.sleep(5);
            }
            assertThat((double) parked / samples).isGreaterThan(0.95);
        }
    }

    private static void waitUntil(BooleanSupplier condition, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("condition was not met within timeout");
    }

    @FunctionalInterface
    private interface BooleanSupplier {
        boolean getAsBoolean();
    }
}
