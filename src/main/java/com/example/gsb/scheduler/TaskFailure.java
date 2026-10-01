package com.example.gsb.scheduler;

/**
 * 失败任务的收集记录。
 */
public record TaskFailure(long taskId, String workerName, Throwable cause) {
}
