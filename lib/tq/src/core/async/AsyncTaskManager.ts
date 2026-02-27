import {Logger, LogLevel} from "@supergrowthai/utils";
import {CronTask} from "../../adapters";
import {IAsyncTaskManager, ShutdownResult} from "./async-task-manager";

const logger = new Logger('AsyncTaskManager', LogLevel.INFO);

const DEFAULT_SWEEP_INTERVAL_MS = 5000;
const DEFAULT_MAX_DURATION_MS = 10 * 60 * 1000; // 10 minutes

export interface AsyncTaskManagerOptions {
    maxTasks?: number;
    /** How often to check for hung tasks (ms). Default: 5000 */
    sweepIntervalMs?: number;
    /** Default max duration for a task before eviction (ms). Default: 10 min */
    defaultMaxDurationMs?: number;
    /** Called when a task is evicted by the sweep. Errors are caught per-task. */
    onTaskTimeout?: (taskId: string, task: CronTask<any>, durationMs: number) => void | Promise<void>;
    /** Grace period for shutdown (ms). Default: 10000 */
    shutdownGracePeriodMs?: number;
}

interface AsyncTaskEntry<ID = any> {
    task: CronTask<ID>;
    promise: Promise<void>;
    startTime: number;
    maxDuration: number;
}

export class AsyncTaskManager<ID = any> implements IAsyncTaskManager<any, ID> {
    private asyncTasks: Map<string, AsyncTaskEntry<ID>> = new Map();
    private handedOffTaskIds: Set<string> = new Set();
    private readonly maxTasks: number;
    private totalHandedOff: number = 0;
    private totalCompleted: number = 0;
    private totalRejected: number = 0;
    private totalTimedOut: number = 0;

    // Sweep
    private sweepInterval: ReturnType<typeof setInterval> | null = null;
    private readonly sweepIntervalMs: number;
    private readonly defaultMaxDurationMs: number;
    private readonly onTaskTimeout?: (taskId: string, task: CronTask<any>, durationMs: number) => void | Promise<void>;
    private readonly shutdownGracePeriodMs: number;

    // Shutdown
    private accepting: boolean = true;

    constructor(maxTasks?: number);
    constructor(options?: AsyncTaskManagerOptions);
    constructor(maxTasksOrOptions?: number | AsyncTaskManagerOptions) {
        let opts: AsyncTaskManagerOptions;
        if (typeof maxTasksOrOptions === 'number') {
            opts = {maxTasks: maxTasksOrOptions};
        } else {
            opts = maxTasksOrOptions || {};
        }

        this.maxTasks = opts.maxTasks ?? 100;
        this.sweepIntervalMs = opts.sweepIntervalMs ?? DEFAULT_SWEEP_INTERVAL_MS;
        this.defaultMaxDurationMs = opts.defaultMaxDurationMs ?? DEFAULT_MAX_DURATION_MS;
        this.onTaskTimeout = opts.onTaskTimeout;
        this.shutdownGracePeriodMs = opts.shutdownGracePeriodMs ?? 10000;

        logger.info(`AsyncTaskManager initialized with max ${this.maxTasks} concurrent tasks`);

        this.startSweep();
    }

    handoffTask(task: CronTask<ID>, runningPromise: Promise<void>, taskTimeout?: number): boolean {
        // Require id for async tasks - we need to track completion in DB
        if (!task.id) {
            logger.error(`Cannot hand off task without id (type: ${task.type})`);
            this.totalRejected++;
            return false;
        }

        const taskId = task.id.toString();

        // Reject if not accepting (shutdown in progress)
        if (!this.accepting) {
            logger.warn(`AsyncTaskManager shutting down, rejecting task ${taskId}`);
            this.totalRejected++;
            return false;
        }

        // Check for duplicate — task already being tracked
        if (this.asyncTasks.has(taskId)) {
            logger.warn(`Duplicate handoff for task ${taskId}, rejecting`);
            this.totalRejected++;
            return false;
        }

        // Check if queue is full
        if (this.asyncTasks.size >= this.maxTasks) {
            logger.warn(`Async queue full (${this.asyncTasks.size}/${this.maxTasks}), rejecting task ${taskId}`);
            this.totalRejected++;
            return false;
        }

        // Add to tracking
        const entry: AsyncTaskEntry<ID> = {
            task: task,
            promise: runningPromise,
            startTime: Date.now(),
            maxDuration: taskTimeout ?? this.defaultMaxDurationMs
        };

        this.asyncTasks.set(taskId, entry);
        this.handedOffTaskIds.add(taskId);
        this.totalHandedOff++;
        logger.info(`Task ${taskId} (${task.type}) handed off to async processing (queue: ${this.asyncTasks.size}/${this.maxTasks})`);

        // Monitor completion - AsyncActions handles success/fail
        // Only increment totalCompleted if the task is still tracked (not evicted by sweep)
        runningPromise
            .then(() => {
                const duration = Date.now() - entry.startTime;
                logger.info(`Async task ${taskId} completed after ${duration}ms`);
                if (this.asyncTasks.has(taskId)) this.totalCompleted++;
            })
            .catch(error => {
                const duration = Date.now() - entry.startTime;
                logger.error(`Async task ${taskId} errored after ${duration}ms:`, error);
                if (this.asyncTasks.has(taskId)) this.totalCompleted++;
            })
            .finally(() => {
                this.asyncTasks.delete(taskId);
                this.handedOffTaskIds.delete(taskId);
                logger.debug(`Task ${taskId} removed from async queue (remaining: ${this.asyncTasks.size})`);
            });

        return true;
    }

    /**
     * Sweep for stale/hung tasks and evict them
     */
    private evictStale(): void {
        const now = Date.now();

        for (const [taskId, entry] of this.asyncTasks) {
            const elapsed = now - entry.startTime;
            if (elapsed > entry.maxDuration) {
                logger.warn(`Evicting stale async task ${taskId} after ${elapsed}ms (max: ${entry.maxDuration}ms)`);

                // Remove from tracking BEFORE callback to free the slot
                this.asyncTasks.delete(taskId);
                this.handedOffTaskIds.delete(taskId);
                this.totalTimedOut++;

                // Fire callback — errors are caught per-task so one failure doesn't block others
                if (this.onTaskTimeout) {
                    try {
                        const result = this.onTaskTimeout(taskId, entry.task, elapsed);
                        if (result instanceof Promise) {
                            result.catch(err => {
                                logger.error(`onTaskTimeout callback error for task ${taskId}:`, err);
                            });
                        }
                    } catch (err) {
                        logger.error(`onTaskTimeout callback error for task ${taskId}:`, err);
                    }
                }
            }
        }
    }

    private startSweep(): void {
        if (this.sweepIntervalMs <= 0) return;
        this.sweepInterval = setInterval(() => {
            this.evictStale();
        }, this.sweepIntervalMs);

        // Unref so it doesn't keep the process alive
        if (this.sweepInterval && typeof this.sweepInterval.unref === 'function') {
            this.sweepInterval.unref();
        }
    }

    async shutdown(abortSignal?: AbortSignal): Promise<ShutdownResult> {
        this.accepting = false;
        logger.info(`Shutting down AsyncTaskManager with ${this.asyncTasks.size} tasks still running`);

        // Stop sweep timer
        if (this.sweepInterval) {
            clearInterval(this.sweepInterval);
            this.sweepInterval = null;
        }

        const initialCount = this.asyncTasks.size;

        if (initialCount === 0) {
            logger.info('AsyncTaskManager shutdown complete (no active tasks)');
            return {completedDuringGrace: 0, abandonedTaskIds: []};
        }

        // Collect all running promises
        const entries = Array.from(this.asyncTasks.entries());
        const promises = entries.map(([, entry]) => entry.promise);

        const gracePeriod = this.shutdownGracePeriodMs;
        logger.info(`Waiting up to ${gracePeriod}ms for ${initialCount} tasks to complete...`);

        // Race: all promises settle OR grace period expires OR abort signal
        const allSettled = Promise.allSettled(promises);
        let graceTimeout: ReturnType<typeof setTimeout>;
        const gracePromise = new Promise<'grace_expired'>((resolve) => {
            graceTimeout = setTimeout(() => resolve('grace_expired'), gracePeriod);

            if (abortSignal?.aborted) {
                clearTimeout(graceTimeout);
                resolve('grace_expired');
                return;
            }

            abortSignal?.addEventListener('abort', () => {
                clearTimeout(graceTimeout);
                resolve('grace_expired');
            });
        });

        await Promise.race([allSettled, gracePromise]);
        clearTimeout(graceTimeout!);

        // Calculate results
        const abandonedTaskIds: string[] = [];
        for (const [taskId] of this.asyncTasks) {
            abandonedTaskIds.push(taskId);
        }

        const completedDuringGrace = initialCount - abandonedTaskIds.length;

        if (abandonedTaskIds.length > 0) {
            logger.warn(`${abandonedTaskIds.length} tasks still running after grace period`);
            for (const [taskId, entry] of this.asyncTasks) {
                const runtime = Date.now() - entry.startTime;
                logger.info(`Task ${taskId} (${entry.task.type}) has been running for ${runtime}ms`);
            }
        }

        logger.info('AsyncTaskManager shutdown complete');
        return {completedDuringGrace, abandonedTaskIds};
    }

    getMetrics() {
        return {
            activeTaskCount: this.asyncTasks.size,
            totalHandedOff: this.totalHandedOff,
            totalCompleted: this.totalCompleted,
            totalRejected: this.totalRejected,
            totalTimedOut: this.totalTimedOut,
            oldestTaskMs: this.getOldestTaskAge(),
            maxTasks: this.maxTasks,
            utilizationPercent: this.maxTasks === 0 ? 0 : (this.asyncTasks.size / this.maxTasks) * 100
        };
    }

    getOldestTaskAge(): number {
        if (this.asyncTasks.size === 0) return 0;

        const now = Date.now();
        let oldest = 0;
        for (const [, entry] of this.asyncTasks) {
            const age = now - entry.startTime;
            if (age > oldest) oldest = age;
        }
        return oldest;
    }

    resetCounters(): void {
        this.totalHandedOff = 0;
        this.totalCompleted = 0;
        this.totalRejected = 0;
        this.totalTimedOut = 0;
    }

    isHandedOff(taskId: ID): boolean {
        return this.handedOffTaskIds.has(String(taskId));
    }

    canAcceptTask(): boolean {
        return this.accepting && this.asyncTasks.size < this.maxTasks;
    }
}
