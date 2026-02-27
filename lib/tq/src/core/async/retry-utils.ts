import {CronTask} from "../../adapters";

const MAX_RETRY_DELAY_MS = 5 * 60 * 1000; // 5 minutes

export interface RetryDecision<ID = any> {
    action: 'retry' | 'fail';
    retryTask?: CronTask<ID>;
}

/**
 * Compute whether a failed task should be retried and produce the retry-ready task.
 *
 * Mirrors the logic in TaskHandler.postProcessTasks() but usable from the async path.
 *
 * @param task       The failed task
 * @param maxRetries Maximum retry attempts for this task type
 * @returns RetryDecision — either { action: 'retry', retryTask } or { action: 'fail' }
 */
export function computeRetryDecision<ID>(
    task: CronTask<ID>,
    maxRetries: number
): RetryDecision<ID> {
    const retryCount = (task.execution_stats && typeof task.execution_stats.retry_count === 'number')
        ? task.execution_stats.retry_count
        : 0;

    if (retryCount >= maxRetries) {
        return {action: 'fail'};
    }

    const taskRetryAfter = Math.max(task.retry_after || 2000, 0);
    const calculatedDelay = taskRetryAfter * Math.pow(retryCount + 1, 2);
    const retryAfter = Math.min(calculatedDelay, MAX_RETRY_DELAY_MS);
    const executeAt = Date.now() + retryAfter;

    return {
        action: 'retry',
        retryTask: {
            ...task,
            status: 'scheduled' as const,
            execute_at: new Date(executeAt),
            execution_stats: {
                ...(task.execution_stats || {}),
                retry_count: retryCount + 1
            }
        }
    };
}
