package com.example.gsb.scheduler;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 单个工作线程的双端任务队列。
 *
 * <ul>
 *   <li>队列的<b>头部</b>只归该工作线程自己使用：本地提交 {@link #pushHead}、本地取任务 {@link #popHead}
 *       （LIFO，对递归 fork 友好）。</li>
 *   <li>其它线程只能从<b>尾部</b>窃取 {@link #stealTail()}（FIFO，取最老的任务）。</li>
 *   <li>带 home 亲和性（{@code pinned}）的任务不会被非归属线程窃取，保证依赖链的本地顺序语义。</li>
 * </ul>
 *
 * 全部操作使用同一把锁串行化，优先保证实现正确、语义清晰。
 */
final class WorkStealingDeque {

    private final ArrayDeque<Task> deque = new ArrayDeque<>();

    synchronized void pushHead(Task task) {
        deque.addFirst(task);
    }

    synchronized Task popHead() {
        return deque.pollFirst();
    }

    /**
     * 从尾部窃取一个可窃取任务：跳过 pinned 到其它 worker 的任务。
     */
    synchronized Task stealTail() {
        Iterator<Task> it = deque.descendingIterator();
        while (it.hasNext()) {
            Task task = it.next();
            if (!task.pinned) {
                it.remove();
                return task;
            }
        }
        return null;
    }

    synchronized boolean isEmpty() {
        return deque.isEmpty();
    }

    synchronized int size() {
        return deque.size();
    }

    /** 线程缩容时原子清空并取出全部剩余任务，由调用方重新分配。 */
    synchronized List<Task> drainAll() {
        List<Task> remaining = new ArrayList<>(deque);
        deque.clear();
        return remaining;
    }
}
