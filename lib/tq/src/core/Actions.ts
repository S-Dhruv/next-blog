import {ExecutorActions} from "./base/interfaces";
import {Logger, LogLevel} from "@supergrowthai/utils";
import {tId} from "../utils/task-id-gen.js";
import {CronTask} from "../adapters";

const logger = new Logger('Actions', LogLevel.INFO);

interface ActionEntry<ID = any> {
    type: 'success' | 'fail' | 'addTasks';
    timestamp: number;
    task?: CronTask<ID>;  // The task passed to success/fail
    newTasks?: CronTask<ID>[];     // Tasks to add
    result?: unknown;              // Result from success(task, result)
    error?: Error | string;        // Error from fail(task, error)
    meta?: Record<string, unknown>; // Meta from fail(task, error, meta)
}

interface TaskContext<ID = any> {
    task: CronTask<ID> | null;  // The task being executed (null for batch contexts)
    actions: ActionEntry<ID>[];
}

export interface ActionResults<ID = any> {
    failedTasks: CronTask<ID>[];
    successTasks: CronTask<ID>[];
    newTasks: CronTask<ID>[];
    ignoredTasks: CronTask<ID>[];
}

const MAX_RESULT_SIZE_BYTES = 256 * 1024; // 256KB

function validateResultSize(result: unknown): boolean {
    // Fast path: null and non-string primitives are always small
    if (result === null || typeof result === 'number' || typeof result === 'boolean') return true;

    try {
        const json = JSON.stringify(result);
        // json.length is always <= Buffer.byteLength (UTF-8 chars are 1-4 bytes)
        // So if json string length exceeds limit, byte length definitely does too
        if (json.length > MAX_RESULT_SIZE_BYTES) return false;
        return Buffer.byteLength(json, 'utf8') <= MAX_RESULT_SIZE_BYTES;
    } catch {
        // Circular reference or other serialization error
        return false;
    }
}

function enrichTaskWithResult<ID>(task: CronTask<ID>, result: unknown): CronTask<ID> {
    if (result === undefined) return task;
    if (!validateResultSize(result)) return task;  // caller logs warning
    task.execution_result = result;  // mutate in place — tasks are already extracted copies
    return task;
}

function enrichTaskWithError<ID>(task: CronTask<ID>, error?: Error | string, meta?: Record<string, unknown>): CronTask<ID> {
    if (!error && !meta) return task;

    const errorFields: Record<string, unknown> = {};
    if (error instanceof Error) {
        errorFields.last_error = error.message;
        errorFields.last_error_stack = error.stack;
    } else if (typeof error === 'string') {
        errorFields.last_error = error;
    }

    // Mutate in place — tasks are already extracted copies
    task.execution_stats = {
        ...(task.execution_stats || {}),
        ...errorFields,
        ...(meta || {})
    };
    return task;
}

export class Actions<ID = any> implements ExecutorActions<ID> {
    private readonly taskRunnerId: string;
    private readonly taskContexts = new Map<string, TaskContext<ID>>();

    /** Logger for multi-task executors — carries runtime-only context (RFC-005) */
    readonly log: Logger;

    constructor(taskRunnerId: string) {
        this.taskRunnerId = taskRunnerId;
        // Root actions logger has no task-specific context — only ALS runtime context applies
        this.log = logger.child({});
    }

    /**
     * Fork execution context for a specific task (for single-task executors)
     */
    forkForTask(task: CronTask<ID>): ExecutorActions<ID> {
        const taskId = tId(task);
        const parentLogContext = task.metadata?.log_context;

        // Initialize context for this task
        const context: TaskContext<ID> = {task, actions: []};
        this.taskContexts.set(taskId, context);

        // Create child logger scoped to this task's log_context (RFC-005)
        const taskLog = logger.child(parentLogContext || {});

        // Return a scoped actions object that tracks everything in this context
        return {
            log: taskLog,

            fail: (t: CronTask<ID>, error?: Error | string, meta?: Record<string, unknown>) => {
                context.actions.push({
                    type: 'fail',
                    timestamp: Date.now(),
                    task: t,
                    error,
                    meta
                });
                logger.error(`[${this.taskRunnerId}] Task failed: ${tId(t)} (${t.type})`);
            },

            success: (t: CronTask<ID>, result?: unknown) => {
                context.actions.push({
                    type: 'success',
                    timestamp: Date.now(),
                    task: t,
                    result
                });
                logger.info(`[${this.taskRunnerId}] Task succeeded: ${tId(t)} (${t.type})`);
            },

            addTasks: (tasks: CronTask<ID>[]) => {
                // Merge parent log_context onto child tasks (RFC-005: parent keys as defaults, child wins)
                const mergedTasks = parentLogContext
                    ? tasks.map(t => ({
                        ...t,
                        metadata: {
                            ...(t.metadata || {}),
                            log_context: {...parentLogContext, ...(t.metadata?.log_context || {})}
                        }
                    }))
                    : tasks;

                context.actions.push({
                    type: 'addTasks',
                    timestamp: Date.now(),
                    newTasks: mergedTasks
                });
                logger.info(`[${this.taskRunnerId}] Task ${taskId} adding ${tasks.length} new tasks`);
            }
        };
    }

    // For multi-task executors - they use the root Actions directly (no forking)
    fail(task: CronTask<ID>, error?: Error | string, meta?: Record<string, unknown>): void {
        const taskId = tId(task);
        let context = this.taskContexts.get(taskId);
        if (!context) {
            context = {task, actions: []};
            this.taskContexts.set(taskId, context);
        }

        context!.actions.push({
            type: 'fail',
            timestamp: Date.now(),
            task,
            error,
            meta
        });
        logger.error(`[${this.taskRunnerId}] Task failed: ${taskId} (${task.type})`);
    }

    success(task: CronTask<ID>, result?: unknown): void {
        const taskId = tId(task);
        let context = this.taskContexts.get(taskId);
        if (!context) {
            context = {task, actions: []};
            this.taskContexts.set(taskId, context);
        }

        context.actions.push({
            type: 'success',
            timestamp: Date.now(),
            task,
            result
        });
        logger.info(`[${this.taskRunnerId}] Task succeeded: ${taskId} (${task.type})`);
    }

    // TODO(P1): Add configurable max child tasks per execution (e.g., 1000).
    //   A buggy executor can call addTasks() with unbounded entries, all in memory.
    //   Each child can spawn more children — unbounded amplification risk.
    addTasks(tasks: CronTask<ID>[]): void {
        // For multi-task mode, store in a batch-specific context
        logger.info(`[${this.taskRunnerId}] Adding ${tasks.length} new tasks`);

        const batchKey = `__batch_${this.taskRunnerId}__`;
        let batchContext = this.taskContexts.get(batchKey);
        if (!batchContext) {
            batchContext = {task: null, actions: []};
            this.taskContexts.set(batchKey, batchContext);
        }
        batchContext.actions.push({
            type: 'addTasks',
            timestamp: Date.now(),
            newTasks: tasks
        });
    }

    addIgnoredTask(task: CronTask<ID>): void {
        const taskId = tId(task);
        this.taskContexts.set(taskId, {task, actions: []});
        logger.warn(`[${this.taskRunnerId}] Task ignored: ${taskId} (${task.type})`);
    }

    /**
     * Check the result status for a specific task
     * Returns 'success', 'fail', or 'pending' (no action recorded yet)
     */
    getTaskResultStatus(taskId: string): 'success' | 'fail' | 'pending' {
        const context = this.taskContexts.get(taskId);
        if (!context) return 'pending';

        for (const action of context.actions) {
            if (action.type === 'success') return 'success';
            if (action.type === 'fail') return 'fail';
        }
        return 'pending';
    }

    /**
     * Extract actions for a single task (used by async tasks)
     */
    extractTaskActions(taskId: string): ActionResults<ID> {
        const results: ActionResults<ID> = {
            failedTasks: [],
            successTasks: [],
            newTasks: [],
            ignoredTasks: []
        };

        const context = this.taskContexts.get(taskId);
        if (!context) return results;

        if (context.actions.length === 0 && context.task) {
            // No actions = ignored task
            results.ignoredTasks.push(context.task);
        } else {
            // Process all actions
            for (const action of context.actions) {
                if (action.type === 'success' && action.task) {
                    if (action.result !== undefined && !validateResultSize(action.result)) {
                        logger.warn(`[${this.taskRunnerId}] Result for task ${tId(action.task)} exceeds size limit, dropping result`);
                    }
                    results.successTasks.push(enrichTaskWithResult(action.task, action.result));
                    // If marking a different task, remove its context
                    const targetTaskId = tId(action.task);
                    if (targetTaskId !== taskId) {
                        this.taskContexts.delete(targetTaskId);
                    }
                } else if (action.type === 'fail' && action.task) {
                    results.failedTasks.push(enrichTaskWithError(action.task, action.error, action.meta));
                    const targetTaskId = tId(action.task);
                    if (targetTaskId !== taskId) {
                        this.taskContexts.delete(targetTaskId);
                    }
                } else if (action.type === 'addTasks' && action.newTasks) {
                    results.newTasks.push(...action.newTasks);
                }
            }
        }

        this.taskContexts.delete(taskId);
        return results;
    }

    /**
     * Extract sync results including batch context (for sync processing)
     */
    extractSyncResults(excludeTaskIds: string[]): ActionResults<ID> {
        const results: ActionResults<ID> = {
            failedTasks: [],
            successTasks: [],
            newTasks: [],
            ignoredTasks: []
        };

        const excludeSet = new Set(excludeTaskIds);
        const batchKey = `__batch_${this.taskRunnerId}__`;

        // Process all task contexts except excluded ones
        for (const [taskId, context] of this.taskContexts) {
            if (excludeSet.has(taskId)) continue;

            if (taskId === batchKey) {
                // Batch context - only has addTasks
                for (const action of context.actions) {
                    if (action.type === 'addTasks' && action.newTasks) {
                        results.newTasks.push(...action.newTasks);
                    }
                }
            } else {
                // Regular task context
                if (context.actions.length === 0 && context.task) {
                    results.ignoredTasks.push(context.task);
                } else {
                    for (const action of context.actions) {
                        if (action.type === 'success' && action.task) {
                            if (action.result !== undefined && !validateResultSize(action.result)) {
                                logger.warn(`[${this.taskRunnerId}] Result for task ${tId(action.task)} exceeds size limit, dropping result`);
                            }
                            results.successTasks.push(enrichTaskWithResult(action.task, action.result));
                        } else if (action.type === 'fail' && action.task) {
                            results.failedTasks.push(enrichTaskWithError(action.task, action.error, action.meta));
                        } else if (action.type === 'addTasks' && action.newTasks) {
                            results.newTasks.push(...action.newTasks);
                        }
                    }
                }
            }
        }

        // Clear processed contexts
        for (const [taskId] of this.taskContexts) {
            if (!excludeSet.has(taskId)) {
                this.taskContexts.delete(taskId);
            }
        }

        return results;
    }

    /**
     * Get all results (mainly for backward compatibility)
     */
    getResults(): ActionResults<ID> {
        return this.extractSyncResults([]);
    }

    /**
     * Get the result for a specific task (before extraction).
     * Used by TaskRunner to pass results to lifecycle events.
     */
    getTaskResult(taskId: string): unknown | undefined {
        const context = this.taskContexts.get(taskId);
        if (!context) return undefined;

        for (const action of context.actions) {
            if (action.type === 'success' && action.result !== undefined) {
                return action.result;
            }
        }
        return undefined;
    }
}