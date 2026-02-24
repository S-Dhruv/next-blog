import {CronTask} from "../../adapters";

/**
 * Result returned from shutdown() describing what happened during grace period
 */
export interface ShutdownResult {
    /** Number of tasks that completed during the grace period */
    completedDuringGrace: number;
    /** Task IDs that were still running when grace period expired */
    abandonedTaskIds: string[];
}

/**
 * Interface for managing async tasks that exceed timeout thresholds
 * Implementation should be in tq package to access executor configs
 */
export interface IAsyncTaskManager<T = any, ID = any> {
    /**
     * Hand off a running task to async management
     * @param message The message that is still being processed
     * @param runningPromise The promise of the still-running task
     * @param taskTimeout Optional per-task timeout override in ms
     * @returns true if accepted, false if queue is full or duplicate
     */
    handoffTask(message: CronTask<ID>, runningPromise: Promise<void>, taskTimeout?: number): boolean;

    /**
     * Gracefully shutdown the async task manager
     * Waits for in-flight promises up to grace period, then returns status
     */
    shutdown(abortSignal?: AbortSignal): Promise<ShutdownResult>;

    isHandedOff(taskId: ID): boolean;

    canAcceptTask(): boolean;

    getMetrics(): {
        activeTaskCount: number;
        totalHandedOff: number;
        totalCompleted: number;
        totalRejected: number;
        totalTimedOut: number;
        oldestTaskMs: number;
        utilizationPercent: number;
        maxTasks: number;
        [key: string]: unknown;
    };

    resetCounters(): void;
}
