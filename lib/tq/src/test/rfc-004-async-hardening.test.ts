import {describe, expect, it, beforeEach, afterEach} from "bun:test";
import {AsyncTaskManager, AsyncTaskManagerOptions} from "../core/async/AsyncTaskManager.js";
import {AsyncActions, AsyncLifecycleEmitter} from "../core/async/AsyncActions.js";
import {Actions} from "../core/Actions.js";
import {CronTask, InMemoryAdapter} from "../adapters";
import {TaskQueuesManager} from "../core/TaskQueuesManager.js";
import {TaskStore} from "../core/TaskStore.js";
import {TaskHandler} from "../core/TaskHandler.js";
import {TaskRunner} from "../core/TaskRunner.js";
import type {QueueName} from "@supergrowthai/mq";
import {InMemoryQueue} from "@supergrowthai/mq";
import type {ISingleTaskNonParallel, IMultiTaskExecutor} from "../core/base/interfaces.js";
import {MemoryCacheProvider} from "memoose-js";
import {computeRetryDecision} from "../core/async/retry-utils.js";
import type {ShutdownResult} from "../core/async/async-task-manager.js";
import type {ITaskLifecycleProvider} from "../core/lifecycle.js";

// ============ Module Declarations ============

declare module "@supergrowthai/mq" {
    interface QueueRegistry {
        "rfc004-queue": "rfc004-queue";
    }

    interface MessagePayloadRegistry {
        "rfc004-task": { input: string };
        "rfc004-task-b": { input: string };
    }
}

const QUEUE: QueueName = "rfc004-queue";

// ============ Shared Test Utilities ============

function makeTask(overrides: Partial<CronTask<string>> = {}): CronTask<string> {
    const id = `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
    return {
        id,
        type: "rfc004-task",
        queue_id: QUEUE,
        payload: {input: "hello"},
        execute_at: new Date(),
        status: "scheduled",
        retries: 0,
        retry_after: 2000,
        created_at: new Date(),
        updated_at: new Date(),
        processing_started_at: new Date(),
        ...overrides
    } as CronTask<string>;
}

function delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function makeManager(opts: Partial<AsyncTaskManagerOptions> = {}): AsyncTaskManager<string> {
    return new AsyncTaskManager<string>({
        maxTasks: opts.maxTasks ?? 10,
        sweepIntervalMs: opts.sweepIntervalMs ?? 0, // disable sweep by default in tests
        defaultMaxDurationMs: opts.defaultMaxDurationMs ?? 60000,
        onTaskTimeout: opts.onTaskTimeout,
        shutdownGracePeriodMs: opts.shutdownGracePeriodMs ?? 5000,
    });
}

function neverResolve(): Promise<void> {
    return new Promise(() => {});
}

// ============ RFC-001F: Metrics Enrichment ============

describe("RFC-004: Async Hardening", () => {

    describe("F: Metrics Enrichment", () => {

        it("F1: full lifecycle metrics (handoff, complete, reject, timeout)", async () => {
            let timedOutTaskId: string | null = null;
            const mgr = makeManager({
                maxTasks: 2,
                sweepIntervalMs: 50,
                defaultMaxDurationMs: 100,
                onTaskTimeout: (taskId) => { timedOutTaskId = taskId; }
            });

            // Handoff a task that will complete
            const t1 = makeTask({id: "f1-complete"});
            const p1 = delay(10).then(() => {});
            expect(mgr.handoffTask(t1, p1)).toBe(true);

            // Handoff a task that will hang and get timed out
            const t2 = makeTask({id: "f1-hung"});
            mgr.handoffTask(t2, neverResolve());

            // Wait for t1 to complete and sweep to evict t2
            await delay(250);

            // Reject one (capacity test - fill then try one more)
            const metrics = mgr.getMetrics();
            expect(metrics.totalHandedOff).toBe(2);
            expect(metrics.totalCompleted).toBe(1);
            expect(metrics.totalTimedOut).toBe(1);
            expect(timedOutTaskId).toBe("f1-hung" as any);

            // Now try a rejection (task without id)
            const t3 = makeTask({id: undefined as any});
            expect(mgr.handoffTask(t3, delay(10))).toBe(false);
            expect(mgr.getMetrics().totalRejected).toBeGreaterThanOrEqual(1);

            await mgr.shutdown();
        });

        it("F2: oldestTaskMs accuracy", async () => {
            const mgr = makeManager({shutdownGracePeriodMs: 100});

            expect(mgr.getOldestTaskAge()).toBe(0);

            const t1 = makeTask({id: "f2-old"});
            mgr.handoffTask(t1, neverResolve());

            await delay(100);

            const age = mgr.getOldestTaskAge();
            expect(age).toBeGreaterThanOrEqual(90);
            expect(age).toBeLessThan(500);

            expect(mgr.getMetrics().oldestTaskMs).toBeGreaterThanOrEqual(90);

            await mgr.shutdown();
        });

        it("F3: resetCounters behavior", async () => {
            const mgr = makeManager({maxTasks: 1});

            const t1 = makeTask({id: "f3-a"});
            const p1 = delay(10);
            mgr.handoffTask(t1, p1);
            await delay(30);

            expect(mgr.getMetrics().totalHandedOff).toBe(1);
            expect(mgr.getMetrics().totalCompleted).toBe(1);

            mgr.resetCounters();

            const metrics = mgr.getMetrics();
            expect(metrics.totalHandedOff).toBe(0);
            expect(metrics.totalCompleted).toBe(0);
            expect(metrics.totalRejected).toBe(0);
            expect(metrics.totalTimedOut).toBe(0);

            await mgr.shutdown();
        });

        it("F4: utilizationPercent at boundaries", () => {
            const mgr = makeManager({maxTasks: 4});

            expect(mgr.getMetrics().utilizationPercent).toBe(0);

            mgr.handoffTask(makeTask({id: "f4-a"}), neverResolve());
            expect(mgr.getMetrics().utilizationPercent).toBe(25);

            mgr.handoffTask(makeTask({id: "f4-b"}), neverResolve());
            expect(mgr.getMetrics().utilizationPercent).toBe(50);

            mgr.handoffTask(makeTask({id: "f4-c"}), neverResolve());
            mgr.handoffTask(makeTask({id: "f4-d"}), neverResolve());
            expect(mgr.getMetrics().utilizationPercent).toBe(100);

            mgr.shutdown();
        });
    });

    // ============ RFC-001C: Duplicate Handoff Guard ============

    describe("C: Duplicate Handoff Guard", () => {

        it("C1: duplicate task ID rejected, metrics not double-counted", () => {
            const mgr = makeManager();

            const t = makeTask({id: "c1-dup"});
            expect(mgr.handoffTask(t, neverResolve())).toBe(true);
            expect(mgr.handoffTask(t, neverResolve())).toBe(false);

            const metrics = mgr.getMetrics();
            expect(metrics.totalHandedOff).toBe(1);
            expect(metrics.activeTaskCount).toBe(1);
            expect(metrics.totalRejected).toBe(1);

            mgr.shutdown();
        });

        it("C2: completed task ID re-handoff accepted", async () => {
            const mgr = makeManager();

            const t = makeTask({id: "c2-reuse"});
            const p = delay(10);
            expect(mgr.handoffTask(t, p)).toBe(true);

            // Wait for completion
            await delay(50);

            // Should be accepted again
            expect(mgr.handoffTask(t, neverResolve())).toBe(true);
            expect(mgr.getMetrics().totalHandedOff).toBe(2);

            mgr.shutdown();
        });

        it("C3: timed-out task ID re-handoff accepted", async () => {
            const mgr = makeManager({
                sweepIntervalMs: 30,
                defaultMaxDurationMs: 50,
            });

            const t = makeTask({id: "c3-timeout"});
            expect(mgr.handoffTask(t, neverResolve())).toBe(true);

            // Wait for sweep eviction
            await delay(150);

            // Should be accepted again after eviction
            expect(mgr.handoffTask(t, neverResolve())).toBe(true);
            expect(mgr.getMetrics().totalHandedOff).toBe(2);
            expect(mgr.getMetrics().totalTimedOut).toBe(1);

            mgr.shutdown();
        });

        it("C4: independent task isolation", () => {
            const mgr = makeManager();

            const t1 = makeTask({id: "c4-a"});
            const t2 = makeTask({id: "c4-b"});

            expect(mgr.handoffTask(t1, neverResolve())).toBe(true);
            expect(mgr.handoffTask(t2, neverResolve())).toBe(true);

            expect(mgr.getMetrics().activeTaskCount).toBe(2);
            expect(mgr.getMetrics().totalHandedOff).toBe(2);

            mgr.shutdown();
        });

        it("C5: no-id task rejected", () => {
            const mgr = makeManager();

            const t = makeTask({id: undefined as any});
            expect(mgr.handoffTask(t, neverResolve())).toBe(false);
            expect(mgr.getMetrics().totalRejected).toBe(1);

            mgr.shutdown();
        });
    });

    // ============ RFC-001A: Per-Task Timeout Sweep ============

    describe("A: Per-Task Timeout Sweep", () => {

        it("A1: hung promise evicted after maxDuration, slot recovered", async () => {
            const evicted: string[] = [];
            const mgr = makeManager({
                maxTasks: 2,
                sweepIntervalMs: 30,
                defaultMaxDurationMs: 80,
                onTaskTimeout: (taskId) => { evicted.push(taskId); }
            });

            const t = makeTask({id: "a1-hung"});
            mgr.handoffTask(t, neverResolve());

            expect(mgr.getMetrics().activeTaskCount).toBe(1);
            await delay(200);

            expect(mgr.getMetrics().activeTaskCount).toBe(0);
            expect(evicted).toContain("a1-hung");

            // Slot should be recovered
            expect(mgr.canAcceptTask()).toBe(true);

            await mgr.shutdown();
        });

        it("A2: full capacity recovery after all promises hang", async () => {
            const mgr = makeManager({
                maxTasks: 3,
                sweepIntervalMs: 30,
                defaultMaxDurationMs: 60,
            });

            mgr.handoffTask(makeTask({id: "a2-a"}), neverResolve());
            mgr.handoffTask(makeTask({id: "a2-b"}), neverResolve());
            mgr.handoffTask(makeTask({id: "a2-c"}), neverResolve());

            expect(mgr.canAcceptTask()).toBe(false);

            await delay(200);

            expect(mgr.canAcceptTask()).toBe(true);
            expect(mgr.getMetrics().activeTaskCount).toBe(0);
            expect(mgr.getMetrics().totalTimedOut).toBe(3);

            await mgr.shutdown();
        });

        it("A3: healthy task not evicted", async () => {
            const evicted: string[] = [];
            const mgr = makeManager({
                sweepIntervalMs: 30,
                defaultMaxDurationMs: 500,
                onTaskTimeout: (taskId) => { evicted.push(taskId); }
            });

            const t = makeTask({id: "a3-healthy"});
            mgr.handoffTask(t, delay(100));

            await delay(200);

            expect(evicted).not.toContain("a3-healthy");
            expect(mgr.getMetrics().totalTimedOut).toBe(0);

            await mgr.shutdown();
        });

        it("A4: per-task timeout honored", async () => {
            const evicted: string[] = [];
            const mgr = makeManager({
                sweepIntervalMs: 30,
                defaultMaxDurationMs: 10000, // default is very long
                onTaskTimeout: (taskId) => { evicted.push(taskId); }
            });

            // Hand off with a short per-task timeout
            const t = makeTask({id: "a4-short"});
            mgr.handoffTask(t, neverResolve(), 80);

            await delay(200);

            expect(evicted).toContain("a4-short");

            await mgr.shutdown();
        });

        it("A5: onTaskTimeout callback throws — sweep continues", async () => {
            let callCount = 0;
            const mgr = makeManager({
                sweepIntervalMs: 30,
                defaultMaxDurationMs: 50,
                onTaskTimeout: () => {
                    callCount++;
                    if (callCount === 1) throw new Error("callback boom");
                }
            });

            mgr.handoffTask(makeTask({id: "a5-throw"}), neverResolve());
            mgr.handoffTask(makeTask({id: "a5-ok"}), neverResolve());

            await delay(200);

            // Both should be evicted even though first callback threw
            expect(mgr.getMetrics().activeTaskCount).toBe(0);
            expect(mgr.getMetrics().totalTimedOut).toBe(2);

            await mgr.shutdown();
        });

        it("A5b: async onTaskTimeout rejection — sweep continues, no unhandled rejection", async () => {
            let callCount = 0;
            const mgr = makeManager({
                sweepIntervalMs: 30,
                defaultMaxDurationMs: 50,
                onTaskTimeout: async () => {
                    callCount++;
                    if (callCount === 1) throw new Error("async callback boom");
                }
            });

            mgr.handoffTask(makeTask({id: "a5b-async-throw"}), neverResolve());
            mgr.handoffTask(makeTask({id: "a5b-ok"}), neverResolve());

            await delay(200);

            // Both evicted even though first async callback rejected
            expect(mgr.getMetrics().activeTaskCount).toBe(0);
            expect(mgr.getMetrics().totalTimedOut).toBe(2);

            await mgr.shutdown();
        });

        it("A6: late-resolving promise after sweep — no crash", async () => {
            let resolveHung: (() => void) | undefined;
            const hungPromise = new Promise<void>(r => { resolveHung = r; });

            const mgr = makeManager({
                sweepIntervalMs: 30,
                defaultMaxDurationMs: 60,
            });

            mgr.handoffTask(makeTask({id: "a6-late"}), hungPromise);
            await delay(150);

            // Should be evicted by now
            expect(mgr.getMetrics().activeTaskCount).toBe(0);

            // Now resolve — should not crash
            resolveHung!();
            await delay(30);

            await mgr.shutdown();
        });

        it("A7: mixed population — only hung tasks evicted", async () => {
            const evicted: string[] = [];
            const mgr = makeManager({
                sweepIntervalMs: 30,
                defaultMaxDurationMs: 80,
                onTaskTimeout: (taskId) => { evicted.push(taskId); }
            });

            // Healthy task - resolves quickly
            mgr.handoffTask(makeTask({id: "a7-healthy"}), delay(20));
            // Hung task
            mgr.handoffTask(makeTask({id: "a7-hung"}), neverResolve());

            await delay(200);

            expect(evicted).toContain("a7-hung");
            expect(evicted).not.toContain("a7-healthy");
            expect(mgr.getMetrics().totalTimedOut).toBe(1);
            expect(mgr.getMetrics().totalCompleted).toBe(1);

            await mgr.shutdown();
        });

        it("A8: sweep timer cleaned up on shutdown", async () => {
            const mgr = makeManager({
                sweepIntervalMs: 30,
                defaultMaxDurationMs: 60,
            });

            await mgr.shutdown();

            // After shutdown, adding a task and waiting should not cause eviction
            // (sweep is stopped) — but handoff is also rejected due to accepting=false
            const t = makeTask({id: "a8-post"});
            expect(mgr.handoffTask(t, neverResolve())).toBe(false);
        });

        it("A9: zero-timeout edge case", async () => {
            const evicted: string[] = [];
            const mgr = makeManager({
                sweepIntervalMs: 30,
                defaultMaxDurationMs: 1000,
                onTaskTimeout: (taskId) => { evicted.push(taskId); }
            });

            // Task with 0 timeout — should be evicted on first sweep
            mgr.handoffTask(makeTask({id: "a9-zero"}), neverResolve(), 0);

            await delay(100);

            expect(evicted).toContain("a9-zero");

            await mgr.shutdown();
        });
    });

    // ============ RFC-001D: Shutdown Awaits In-Flight Promises ============

    describe("D: Shutdown Awaits In-Flight Promises", () => {

        it("D1: returns in ~200ms when promise settles in 200ms", async () => {
            const mgr = makeManager({shutdownGracePeriodMs: 5000});

            mgr.handoffTask(makeTask({id: "d1-fast"}), delay(200));

            const start = Date.now();
            const result = await mgr.shutdown();
            const elapsed = Date.now() - start;

            expect(result.completedDuringGrace).toBe(1);
            expect(result.abandonedTaskIds).toHaveLength(0);
            expect(elapsed).toBeLessThan(1000);
        });

        it("D2: returns abandoned task IDs after grace period", async () => {
            const mgr = makeManager({shutdownGracePeriodMs: 100});

            mgr.handoffTask(makeTask({id: "d2-hung"}), neverResolve());

            const result = await mgr.shutdown();

            expect(result.abandonedTaskIds).toContain("d2-hung");
            expect(result.completedDuringGrace).toBe(0);
        });

        it("D3: immediate return with no active tasks", async () => {
            const mgr = makeManager();

            const start = Date.now();
            const result = await mgr.shutdown();
            const elapsed = Date.now() - start;

            expect(result.completedDuringGrace).toBe(0);
            expect(result.abandonedTaskIds).toHaveLength(0);
            expect(elapsed).toBeLessThan(50);
        });

        it("D4: mixed settled/abandoned — correct counts", async () => {
            const mgr = makeManager({shutdownGracePeriodMs: 300});

            mgr.handoffTask(makeTask({id: "d4-fast"}), delay(50));
            mgr.handoffTask(makeTask({id: "d4-hung"}), neverResolve());

            const result = await mgr.shutdown();

            expect(result.completedDuringGrace).toBe(1);
            expect(result.abandonedTaskIds).toContain("d4-hung");
            expect(result.abandonedTaskIds).not.toContain("d4-fast");
        });

        it("D5: rejected promise counts as settled", async () => {
            const mgr = makeManager({shutdownGracePeriodMs: 5000});

            const rejectingPromise = delay(50).then(() => { throw new Error("boom"); });
            mgr.handoffTask(makeTask({id: "d5-reject"}), rejectingPromise);

            const result = await mgr.shutdown();

            expect(result.completedDuringGrace).toBe(1);
            expect(result.abandonedTaskIds).toHaveLength(0);
        });

        it("D6: no new handoffs after shutdown begins", async () => {
            const mgr = makeManager({shutdownGracePeriodMs: 200});

            // Start shutdown in background
            const shutdownPromise = mgr.shutdown();

            // Try to handoff during shutdown
            const t = makeTask({id: "d6-late"});
            expect(mgr.handoffTask(t, delay(10))).toBe(false);
            expect(mgr.canAcceptTask()).toBe(false);

            await shutdownPromise;
        });
    });

    // ============ RFC-001B: Async Task Retry Pipeline ============

    describe("B: Async Task Retry Pipeline", () => {

        it("B1: async task failure retried with backoff", async () => {
            const databaseAdapter = new InMemoryAdapter();
            const messageQueue = new InMemoryQueue();
            const taskQueue = new TaskQueuesManager<string>(messageQueue);
            const taskStore = new TaskStore<string>(databaseAdapter);

            const executor: ISingleTaskNonParallel<string, "rfc004-task"> = {
                multiple: false,
                parallel: false,
                store_on_failure: true,
                default_retries: 3,
            } as any;
            taskQueue.register(QUEUE, "rfc004-task", executor);

            const task = makeTask({
                id: "b1-retry",
                retries: 3,
                retry_after: 1000,
                execution_stats: {retry_count: 0}
            });
            await databaseAdapter.addTasksToScheduled([task]);

            const actions = new Actions<string>("test-b1");
            const forked = actions.forkForTask(task);
            forked.fail(task, new Error("transient error"));

            const asyncActions = new AsyncActions<string>(
                messageQueue, taskStore, taskQueue, actions, task, () => databaseAdapter.generateId()
            );

            await asyncActions.onPromiseFulfilled();

            // Should have been retried (upserted with retry_count: 1)
            const stored = await databaseAdapter.getTasksByIds(["b1-retry"]);
            expect(stored).toHaveLength(1);
            expect(stored[0].status).toBe("scheduled");
            expect(stored[0].execution_stats?.retry_count).toBe(1);
        });

        it("B2: forgetful executor — task marked failed", async () => {
            const databaseAdapter = new InMemoryAdapter();
            const messageQueue = new InMemoryQueue();
            const taskQueue = new TaskQueuesManager<string>(messageQueue);
            const taskStore = new TaskStore<string>(databaseAdapter);

            // Executor with 0 retries
            const executor: ISingleTaskNonParallel<string, "rfc004-task"> = {
                multiple: false,
                parallel: false,
                store_on_failure: true,
                default_retries: 0,
            } as any;
            taskQueue.register(QUEUE, "rfc004-task", executor);

            const task = makeTask({id: "b2-forget", retries: 0});
            await databaseAdapter.addTasksToScheduled([task]);

            // No success() or fail() called — empty actions
            const actions = new Actions<string>("test-b2");
            actions.forkForTask(task); // fork but don't call anything

            const asyncActions = new AsyncActions<string>(
                messageQueue, taskStore, taskQueue, actions, task, () => databaseAdapter.generateId()
            );

            // Should NOT throw — should default to fail
            await asyncActions.onPromiseFulfilled();

            const stored = await databaseAdapter.getTasksByIds(["b2-forget"]);
            expect(stored).toHaveLength(1);
            expect(stored[0].status).toBe("failed");
        });

        it("B3: forgetful executor with retries — enters retry pipeline", async () => {
            const databaseAdapter = new InMemoryAdapter();
            const messageQueue = new InMemoryQueue();
            const taskQueue = new TaskQueuesManager<string>(messageQueue);
            const taskStore = new TaskStore<string>(databaseAdapter);

            const executor: ISingleTaskNonParallel<string, "rfc004-task"> = {
                multiple: false,
                parallel: false,
                store_on_failure: true,
                default_retries: 2,
            } as any;
            taskQueue.register(QUEUE, "rfc004-task", executor);

            const task = makeTask({id: "b3-forget-retry", retries: 2});
            await databaseAdapter.addTasksToScheduled([task]);

            const actions = new Actions<string>("test-b3");
            actions.forkForTask(task); // fork but don't call anything

            const asyncActions = new AsyncActions<string>(
                messageQueue, taskStore, taskQueue, actions, task, () => databaseAdapter.generateId()
            );

            await asyncActions.onPromiseFulfilled();

            // Should be retried, not failed
            const stored = await databaseAdapter.getTasksByIds(["b3-forget-retry"]);
            expect(stored).toHaveLength(1);
            expect(stored[0].status).toBe("scheduled");
            expect(stored[0].execution_stats?.retry_count).toBe(1);
        });

        it("B3b: task.retries undefined — falls back to executor.default_retries", async () => {
            const databaseAdapter = new InMemoryAdapter();
            const messageQueue = new InMemoryQueue();
            const taskQueue = new TaskQueuesManager<string>(messageQueue);
            const taskStore = new TaskStore<string>(databaseAdapter);

            const executor: ISingleTaskNonParallel<string, "rfc004-task"> = {
                multiple: false,
                parallel: false,
                store_on_failure: true,
                default_retries: 3,
            } as any;
            taskQueue.register(QUEUE, "rfc004-task", executor);

            // task.retries is undefined — retry logic must fall back to executor.default_retries (3)
            const task = makeTask({id: "b3b-fallback", retries: undefined as any, execution_stats: {retry_count: 0}});
            await databaseAdapter.addTasksToScheduled([task]);

            const actions = new Actions<string>("test-b3b");
            const forked = actions.forkForTask(task);
            forked.fail(task, new Error("should retry via executor config"));

            const asyncActions = new AsyncActions<string>(
                messageQueue, taskStore, taskQueue, actions, task, () => databaseAdapter.generateId()
            );

            await asyncActions.onPromiseFulfilled();

            const stored = await databaseAdapter.getTasksByIds(["b3b-fallback"]);
            expect(stored).toHaveLength(1);
            // Must be scheduled (retried), NOT failed — proves executor.default_retries was used
            expect(stored[0].status).toBe("scheduled");
            expect(stored[0].execution_stats?.retry_count).toBe(1);
        });

        it("B4: computeRetryDecision boundary conditions", () => {
            const task = makeTask({
                id: "b4-boundary",
                retry_after: 2000,
                execution_stats: {retry_count: 2}
            });

            // At boundary: retryCount(2) < maxRetries(3) → retry
            const d1 = computeRetryDecision(task, 3);
            expect(d1.action).toBe("retry");
            expect(d1.retryTask?.execution_stats?.retry_count).toBe(3);

            // At boundary: retryCount(2) >= maxRetries(2) → fail
            const d2 = computeRetryDecision(task, 2);
            expect(d2.action).toBe("fail");

            // Zero retries
            const taskZero = makeTask({id: "b4-zero", execution_stats: {retry_count: 0}});
            const d3 = computeRetryDecision(taskZero, 0);
            expect(d3.action).toBe("fail");
        });

        it("B5: backoff delay capped", () => {
            const task = makeTask({
                id: "b5-cap",
                retry_after: 60000, // 60s base
                execution_stats: {retry_count: 10}
            });

            const decision = computeRetryDecision(task, 100);
            expect(decision.action).toBe("retry");

            // Calculate expected: 60000 * (10+1)^2 = 60000 * 121 = 7260000 → capped at 300000
            const executeAtMs = decision.retryTask!.execute_at.getTime();
            const expectedMin = Date.now() + 300_000 - 1000; // allow 1s tolerance
            const expectedMax = Date.now() + 300_000 + 1000;
            expect(executeAtMs).toBeGreaterThanOrEqual(expectedMin);
            expect(executeAtMs).toBeLessThanOrEqual(expectedMax);
        });

        it("B6: missing retry_count treated as 0", () => {
            const task = makeTask({
                id: "b6-missing",
                execution_stats: {} // no retry_count
            });

            const decision = computeRetryDecision(task, 3);
            expect(decision.action).toBe("retry");
            expect(decision.retryTask?.execution_stats?.retry_count).toBe(1);
        });

        it("B7: preserves existing execution_stats", () => {
            const task = makeTask({
                id: "b7-preserve",
                retry_after: 2000,
                execution_stats: {
                    retry_count: 1,
                    last_error: "previous error",
                    custom_field: "keep_me"
                }
            });

            const decision = computeRetryDecision(task, 5);
            expect(decision.action).toBe("retry");
            expect(decision.retryTask?.execution_stats?.last_error).toBe("previous error");
            expect(decision.retryTask?.execution_stats?.custom_field).toBe("keep_me");
            expect(decision.retryTask?.execution_stats?.retry_count).toBe(2);
        });

        it("B8: async success not interfered with", async () => {
            const databaseAdapter = new InMemoryAdapter();
            const messageQueue = new InMemoryQueue();
            const taskQueue = new TaskQueuesManager<string>(messageQueue);
            const taskStore = new TaskStore<string>(databaseAdapter);

            const executor: ISingleTaskNonParallel<string, "rfc004-task"> = {
                multiple: false,
                parallel: false,
                store_on_failure: true,
                default_retries: 3,
            } as any;
            taskQueue.register(QUEUE, "rfc004-task", executor);

            const task = makeTask({id: "b8-success"});
            await databaseAdapter.addTasksToScheduled([task]);

            const actions = new Actions<string>("test-b8");
            const forked = actions.forkForTask(task);
            forked.success(task, {result: "ok"});

            const asyncActions = new AsyncActions<string>(
                messageQueue, taskStore, taskQueue, actions, task, () => databaseAdapter.generateId()
            );

            await asyncActions.onPromiseFulfilled();

            const stored = await databaseAdapter.getTasksByIds(["b8-success"]);
            expect(stored).toHaveLength(1);
            expect(stored[0].status).toBe("executed");
            expect(stored[0].execution_result).toEqual({result: "ok"});
        });
    });

    // ============ RFC-001E: Async Lifecycle Events ============

    describe("E: Async Lifecycle Events", () => {

        it("E1: async completion fires onCompleted", async () => {
            const databaseAdapter = new InMemoryAdapter();
            const messageQueue = new InMemoryQueue();
            const taskQueue = new TaskQueuesManager<string>(messageQueue);
            const taskStore = new TaskStore<string>(databaseAdapter);

            const executor: ISingleTaskNonParallel<string, "rfc004-task"> = {
                multiple: false,
                parallel: false,
                store_on_failure: true,
            } as any;
            taskQueue.register(QUEUE, "rfc004-task", executor);

            let completedTask: CronTask<string> | null = null;
            let completedResult: unknown = undefined;

            const emitter: AsyncLifecycleEmitter = {
                onCompleted: (task, result) => {
                    completedTask = task;
                    completedResult = result;
                },
                onFailed: () => {}
            };

            const task = makeTask({id: "e1-complete"});
            await databaseAdapter.addTasksToScheduled([task]);

            const actions = new Actions<string>("test-e1");
            const forked = actions.forkForTask(task);
            forked.success(task, {lifecycle: "works"});

            const asyncActions = new AsyncActions<string>(
                messageQueue, taskStore, taskQueue, actions, task, () => databaseAdapter.generateId(), emitter
            );

            await asyncActions.onPromiseFulfilled();

            expect(completedTask).not.toBeNull();
            expect(completedResult).toEqual({lifecycle: "works"});
        });

        it("E2: async failure fires onFailed with willRetry", async () => {
            const databaseAdapter = new InMemoryAdapter();
            const messageQueue = new InMemoryQueue();
            const taskQueue = new TaskQueuesManager<string>(messageQueue);
            const taskStore = new TaskStore<string>(databaseAdapter);

            const executor: ISingleTaskNonParallel<string, "rfc004-task"> = {
                multiple: false,
                parallel: false,
                store_on_failure: true,
                default_retries: 3,
            } as any;
            taskQueue.register(QUEUE, "rfc004-task", executor);

            let failedWillRetry: boolean | null = null;

            const emitter: AsyncLifecycleEmitter = {
                onCompleted: () => {},
                onFailed: (_task, _error, willRetry) => {
                    failedWillRetry = willRetry;
                }
            };

            const task = makeTask({id: "e2-fail", retries: 3, execution_stats: {retry_count: 0}});
            await databaseAdapter.addTasksToScheduled([task]);

            const actions = new Actions<string>("test-e2");
            const forked = actions.forkForTask(task);
            forked.fail(task, new Error("e2 error"));

            const asyncActions = new AsyncActions<string>(
                messageQueue, taskStore, taskQueue, actions, task, () => databaseAdapter.generateId(), emitter
            );

            await asyncActions.onPromiseFulfilled();

            expect(failedWillRetry).toBe(true as any);
        });

        it("E3: optional — no crash without lifecycle provider", async () => {
            const databaseAdapter = new InMemoryAdapter();
            const messageQueue = new InMemoryQueue();
            const taskQueue = new TaskQueuesManager<string>(messageQueue);
            const taskStore = new TaskStore<string>(databaseAdapter);

            const executor: ISingleTaskNonParallel<string, "rfc004-task"> = {
                multiple: false,
                parallel: false,
                store_on_failure: true,
            } as any;
            taskQueue.register(QUEUE, "rfc004-task", executor);

            const task = makeTask({id: "e3-no-emitter"});
            await databaseAdapter.addTasksToScheduled([task]);

            const actions = new Actions<string>("test-e3");
            const forked = actions.forkForTask(task);
            forked.success(task, {ok: true});

            // No emitter passed — should not crash
            const asyncActions = new AsyncActions<string>(
                messageQueue, taskStore, taskQueue, actions, task, () => databaseAdapter.generateId()
            );

            await asyncActions.onPromiseFulfilled();

            const stored = await databaseAdapter.getTasksByIds(["e3-no-emitter"]);
            expect(stored[0].status).toBe("executed");
        });
    });

    // ============ Integration & Cross-Cutting Tests ============

    describe("G: End-to-End Pipeline", () => {

        it("G1: full async pipeline — handoff, complete, verify DB", async () => {
            const messageQueue = new InMemoryQueue();
            const databaseAdapter = new InMemoryAdapter();
            const taskQueue = new TaskQueuesManager<string>(messageQueue);
            const cacheProvider = new MemoryCacheProvider();

            const asyncTaskManager = new AsyncTaskManager<string>({maxTasks: 10, sweepIntervalMs: 0});

            const taskHandler = new TaskHandler<string>(
                messageQueue, taskQueue, databaseAdapter, cacheProvider, asyncTaskManager
            );

            const task = makeTask({id: "g1-e2e"});
            await databaseAdapter.addTasksToScheduled([task]);

            const executor: ISingleTaskNonParallel<string, "rfc004-task"> = {
                multiple: false,
                parallel: false,
                store_on_failure: true,
                asyncConfig: {handoffTimeout: 50},
                onTask: async (t, actions) => {
                    await delay(200);
                    actions.success(t, {e2e: "result"});
                }
            };

            taskQueue.register(QUEUE, "rfc004-task", executor);

            await messageQueue.addMessages(QUEUE, [task]);
            await taskHandler.startConsumingTasks(QUEUE);
            await delay(2000);

            const stored = await databaseAdapter.getTasksByIds(["g1-e2e"]);
            expect(stored).toHaveLength(1);
            expect(stored[0].status).toBe("executed");
            expect(stored[0].execution_result).toEqual({e2e: "result"});

            await asyncTaskManager.shutdown();
            await messageQueue.shutdown();
        }, 10000);

        it("G2: async task produces child tasks", async () => {
            const messageQueue = new InMemoryQueue();
            const databaseAdapter = new InMemoryAdapter();
            const taskQueue = new TaskQueuesManager<string>(messageQueue);
            const cacheProvider = new MemoryCacheProvider();

            const asyncTaskManager = new AsyncTaskManager<string>({maxTasks: 10, sweepIntervalMs: 0});

            const taskHandler = new TaskHandler<string>(
                messageQueue, taskQueue, databaseAdapter, cacheProvider, asyncTaskManager
            );

            const parentTask = makeTask({id: "g2-parent"});
            await databaseAdapter.addTasksToScheduled([parentTask]);

            let childReceived = false;
            const executor: ISingleTaskNonParallel<string, "rfc004-task"> = {
                multiple: false,
                parallel: false,
                store_on_failure: true,
                asyncConfig: {handoffTimeout: 50},
                onTask: async (t, actions) => {
                    if ((t.payload as any).input === "hello") {
                        await delay(150);
                        actions.success(t, {parent: true});
                        actions.addTasks([makeTask({
                            id: "g2-child",
                            type: "rfc004-task",
                            payload: {input: "child"}
                        })]);
                    } else {
                        childReceived = true;
                        actions.success(t);
                    }
                }
            };

            taskQueue.register(QUEUE, "rfc004-task", executor);

            await messageQueue.addMessages(QUEUE, [parentTask]);
            await taskHandler.startConsumingTasks(QUEUE);
            await delay(3000);

            const stored = await databaseAdapter.getTasksByIds(["g2-parent"]);
            expect(stored[0].status).toBe("executed");
            expect(childReceived).toBe(true);

            await asyncTaskManager.shutdown();
            await messageQueue.shutdown();
        }, 10000);

        it("G3: full-queue rejects handoff, task requeued", async () => {
            const mgr = makeManager({maxTasks: 1, shutdownGracePeriodMs: 100});

            // Fill the queue
            mgr.handoffTask(makeTask({id: "g3-fill"}), neverResolve());

            // Second handoff should fail
            const t = makeTask({id: "g3-overflow"});
            expect(mgr.handoffTask(t, delay(10))).toBe(false);

            await mgr.shutdown();
        });

        it("G4: concurrent handoffs — no race corruption", async () => {
            const mgr = makeManager({maxTasks: 100});

            const results: boolean[] = [];
            for (let i = 0; i < 50; i++) {
                const t = makeTask({id: `g4-${i}`});
                results.push(mgr.handoffTask(t, delay(10)));
            }

            expect(results.filter(r => r).length).toBe(50);
            expect(mgr.getMetrics().totalHandedOff).toBe(50);

            await delay(100);
            await mgr.shutdown();
        });

        it("G5: throwing executor — async task still completes", async () => {
            const databaseAdapter = new InMemoryAdapter();
            const messageQueue = new InMemoryQueue();
            const taskQueue = new TaskQueuesManager<string>(messageQueue);
            const taskStore = new TaskStore<string>(databaseAdapter);
            const cacheProvider = new MemoryCacheProvider();

            const asyncTaskManager = new AsyncTaskManager<string>({maxTasks: 10, sweepIntervalMs: 0});

            const taskHandler = new TaskHandler<string>(
                messageQueue, taskQueue, databaseAdapter, cacheProvider, asyncTaskManager
            );

            const task = makeTask({id: "g5-throw", retries: 0});
            await databaseAdapter.addTasksToScheduled([task]);

            const executor: ISingleTaskNonParallel<string, "rfc004-task"> = {
                multiple: false,
                parallel: false,
                store_on_failure: true,
                default_retries: 0,
                asyncConfig: {handoffTimeout: 50},
                onTask: async (_t, _actions) => {
                    await delay(150);
                    throw new Error("executor crashed");
                }
            };

            taskQueue.register(QUEUE, "rfc004-task", executor);

            await messageQueue.addMessages(QUEUE, [task]);
            await taskHandler.startConsumingTasks(QUEUE);
            await delay(2000);

            // The task should be marked as failed (crash caught by TaskRunner, default-to-fail in AsyncActions)
            const stored = await databaseAdapter.getTasksByIds(["g5-throw"]);
            expect(stored).toHaveLength(1);
            expect(stored[0].status).toBe("failed");

            await asyncTaskManager.shutdown();
            await messageQueue.shutdown();
        }, 10000);

        it("G6: abort signal interrupts shutdown grace period", async () => {
            const mgr = makeManager({shutdownGracePeriodMs: 10000});
            mgr.handoffTask(makeTask({id: "g6-hung"}), neverResolve());

            const abortController = new AbortController();
            setTimeout(() => abortController.abort(), 100);

            const start = Date.now();
            const result = await mgr.shutdown(abortController.signal);
            const elapsed = Date.now() - start;

            expect(elapsed).toBeLessThan(500);
            expect(result.abandonedTaskIds).toContain("g6-hung");
        });
    });

    describe("H: Error Scenarios", () => {

        it("H1: DB failure during async completion — error propagated", async () => {
            const databaseAdapter = new InMemoryAdapter();
            const messageQueue = new InMemoryQueue();
            const taskQueue = new TaskQueuesManager<string>(messageQueue);
            const taskStore = new TaskStore<string>(databaseAdapter);

            const executor: ISingleTaskNonParallel<string, "rfc004-task"> = {
                multiple: false,
                parallel: false,
                store_on_failure: true,
            } as any;
            taskQueue.register(QUEUE, "rfc004-task", executor);

            const task = makeTask({id: "h1-db-fail"});
            await databaseAdapter.addTasksToScheduled([task]);

            const actions = new Actions<string>("test-h1");
            const forked = actions.forkForTask(task);
            forked.success(task, {ok: true});

            // Sabotage the DB
            const origMarkSuccess = databaseAdapter.markTasksAsExecuted.bind(databaseAdapter);
            databaseAdapter.markTasksAsExecuted = async () => { throw new Error("DB down"); };

            const asyncActions = new AsyncActions<string>(
                messageQueue, taskStore, taskQueue, actions, task, () => databaseAdapter.generateId()
            );

            await expect(asyncActions.onPromiseFulfilled()).rejects.toThrow("DB down");

            // Restore
            databaseAdapter.markTasksAsExecuted = origMarkSuccess;
        });

        it("H2: success then throw in executor — success still persisted", async () => {
            const databaseAdapter = new InMemoryAdapter();
            const messageQueue = new InMemoryQueue();
            const taskQueue = new TaskQueuesManager<string>(messageQueue);
            const taskStore = new TaskStore<string>(databaseAdapter);

            const executor: ISingleTaskNonParallel<string, "rfc004-task"> = {
                multiple: false,
                parallel: false,
                store_on_failure: true,
            } as any;
            taskQueue.register(QUEUE, "rfc004-task", executor);

            const task = makeTask({id: "h2-success-throw"});
            await databaseAdapter.addTasksToScheduled([task]);

            const actions = new Actions<string>("test-h2");
            const forked = actions.forkForTask(task);
            // Executor calls success, then throws
            forked.success(task, {saved: true});

            const asyncActions = new AsyncActions<string>(
                messageQueue, taskStore, taskQueue, actions, task, () => databaseAdapter.generateId()
            );

            await asyncActions.onPromiseFulfilled();

            const stored = await databaseAdapter.getTasksByIds(["h2-success-throw"]);
            expect(stored[0].execution_result).toEqual({saved: true});
            expect(stored[0].status).toBe("executed");
        });

        it("H3: error context preserved through async retry", async () => {
            const databaseAdapter = new InMemoryAdapter();
            const messageQueue = new InMemoryQueue();
            const taskQueue = new TaskQueuesManager<string>(messageQueue);
            const taskStore = new TaskStore<string>(databaseAdapter);

            const executor: ISingleTaskNonParallel<string, "rfc004-task"> = {
                multiple: false,
                parallel: false,
                store_on_failure: true,
                default_retries: 3,
            } as any;
            taskQueue.register(QUEUE, "rfc004-task", executor);

            const task = makeTask({
                id: "h3-context",
                retries: 3,
                execution_stats: {
                    retry_count: 0,
                    last_error: "previous error",
                    custom: "data"
                }
            });
            await databaseAdapter.addTasksToScheduled([task]);

            const actions = new Actions<string>("test-h3");
            const forked = actions.forkForTask(task);
            forked.fail(task, new Error("new error"), {extra: "meta"});

            const asyncActions = new AsyncActions<string>(
                messageQueue, taskStore, taskQueue, actions, task, () => databaseAdapter.generateId()
            );

            await asyncActions.onPromiseFulfilled();

            const stored = await databaseAdapter.getTasksByIds(["h3-context"]);
            expect(stored[0].execution_stats?.last_error).toBe("new error");
            expect(stored[0].execution_stats?.extra).toBe("meta");
            expect(stored[0].execution_stats?.retry_count).toBe(1);
        });

        it("H4: sweep/late-settle race — no double-count", async () => {
            let resolveTask: (() => void) | undefined;
            const taskPromise = new Promise<void>(r => { resolveTask = r; });

            const mgr = makeManager({
                sweepIntervalMs: 30,
                defaultMaxDurationMs: 60,
            });

            mgr.handoffTask(makeTask({id: "h4-race"}), taskPromise);
            await delay(150); // sweep evicts

            expect(mgr.getMetrics().activeTaskCount).toBe(0);

            // Late resolve — should not crash or corrupt
            resolveTask!();
            await delay(30);

            // Should have 1 timeout, and the late resolve's .then handler will fire
            // but the task is already removed from the map — so totalCompleted must NOT increment
            expect(mgr.getMetrics().totalTimedOut).toBe(1);
            expect(mgr.getMetrics().totalCompleted).toBe(0);

            await mgr.shutdown();
        });

        it("H5: success + addTasks atomicity", async () => {
            const databaseAdapter = new InMemoryAdapter();
            const messageQueue = new InMemoryQueue();
            const taskQueue = new TaskQueuesManager<string>(messageQueue);
            const taskStore = new TaskStore<string>(databaseAdapter);

            const executor: ISingleTaskNonParallel<string, "rfc004-task"> = {
                multiple: false,
                parallel: false,
                store_on_failure: true,
            } as any;
            taskQueue.register(QUEUE, "rfc004-task", executor);

            const task = makeTask({id: "h5-atomic"});
            await databaseAdapter.addTasksToScheduled([task]);

            const actions = new Actions<string>("test-h5");
            const forked = actions.forkForTask(task);
            forked.success(task, {done: true});
            forked.addTasks([makeTask({
                id: "h5-child",
                execute_at: new Date(Date.now() + 3 * 60 * 1000) // future task
            })]);

            const asyncActions = new AsyncActions<string>(
                messageQueue, taskStore, taskQueue, actions, task, () => databaseAdapter.generateId()
            );

            await asyncActions.onPromiseFulfilled();

            const stored = await databaseAdapter.getTasksByIds(["h5-atomic"]);
            expect(stored[0].status).toBe("executed");

            // Child should be scheduled in DB (future task)
            const allTasks = await databaseAdapter.getTasksByIds(["h5-child"]);
            expect(allTasks).toHaveLength(1);
        });
    });

    describe("I: Race Conditions", () => {

        it("I1: handoff timeout boundary race", async () => {
            const mgr = makeManager({maxTasks: 10});

            // Task that resolves right at the edge
            const task = makeTask({id: "i1-boundary"});
            const result = mgr.handoffTask(task, delay(1));

            expect(result).toBe(true);
            await delay(50);

            expect(mgr.getMetrics().totalCompleted).toBe(1);
            await mgr.shutdown();
        });

        it("I2: staggered eviction — tasks evicted in order", async () => {
            const evicted: string[] = [];
            const mgr = makeManager({
                sweepIntervalMs: 30,
                defaultMaxDurationMs: 10000,
                onTaskTimeout: (taskId) => { evicted.push(taskId); }
            });

            // Short timeout
            mgr.handoffTask(makeTask({id: "i2-short"}), neverResolve(), 50);
            // Long timeout
            mgr.handoffTask(makeTask({id: "i2-long"}), neverResolve(), 300);

            await delay(100);
            expect(evicted).toContain("i2-short");
            expect(evicted).not.toContain("i2-long");

            await delay(300);
            expect(evicted).toContain("i2-long");

            await mgr.shutdown();
        });

        it("I3: immediate child MQ routing from async", async () => {
            const databaseAdapter = new InMemoryAdapter();
            const messageQueue = new InMemoryQueue();
            const taskQueue = new TaskQueuesManager<string>(messageQueue);
            const taskStore = new TaskStore<string>(databaseAdapter);

            const executor: ISingleTaskNonParallel<string, "rfc004-task"> = {
                multiple: false,
                parallel: false,
                store_on_failure: true,
            } as any;
            taskQueue.register(QUEUE, "rfc004-task", executor);

            const task = makeTask({id: "i3-parent"});
            await databaseAdapter.addTasksToScheduled([task]);

            const actions = new Actions<string>("test-i3");
            const forked = actions.forkForTask(task);
            forked.success(task);
            // Add an immediate child task (execute_at = now)
            forked.addTasks([makeTask({id: "i3-child-immediate"})]);

            const asyncActions = new AsyncActions<string>(
                messageQueue, taskStore, taskQueue, actions, task, () => databaseAdapter.generateId()
            );

            await asyncActions.onPromiseFulfilled();

            // Parent should be completed
            const stored = await databaseAdapter.getTasksByIds(["i3-parent"]);
            expect(stored[0].status).toBe("executed");
        });
    });

    describe("J: Structural Invariants", () => {

        it("J1: Map/Set sync — handedOffTaskIds mirrors asyncTasks", async () => {
            const mgr = makeManager();

            const t1 = makeTask({id: "j1-a"});
            const p1 = delay(50);
            mgr.handoffTask(t1, p1);

            expect(mgr.isHandedOff("j1-a" as any)).toBe(true);
            expect(mgr.getMetrics().activeTaskCount).toBe(1);

            await delay(100);

            expect(mgr.isHandedOff("j1-a" as any)).toBe(false);
            expect(mgr.getMetrics().activeTaskCount).toBe(0);

            await mgr.shutdown();
        });

        it("J2: mixed sync/async batch — no cross-contamination", async () => {
            const messageQueue = new InMemoryQueue();
            const databaseAdapter = new InMemoryAdapter();
            const taskQueue = new TaskQueuesManager<string>(messageQueue);
            const cacheProvider = new MemoryCacheProvider();

            const asyncTaskManager = new AsyncTaskManager<string>({maxTasks: 10, sweepIntervalMs: 0});

            const taskHandler = new TaskHandler<string>(
                messageQueue, taskQueue, databaseAdapter, cacheProvider, asyncTaskManager
            );

            const syncTask = makeTask({id: "j2-sync", type: "rfc004-task-b" as any});
            const asyncTask = makeTask({id: "j2-async"});

            await databaseAdapter.addTasksToScheduled([syncTask, asyncTask]);

            // Sync executor
            const syncExec: ISingleTaskNonParallel<string, "rfc004-task-b"> = {
                multiple: false,
                parallel: false,
                store_on_failure: true,
                onTask: async (t, actions) => {
                    actions.success(t, {sync: true});
                }
            };

            // Async executor
            const asyncExec: ISingleTaskNonParallel<string, "rfc004-task"> = {
                multiple: false,
                parallel: false,
                store_on_failure: true,
                asyncConfig: {handoffTimeout: 50},
                onTask: async (t, actions) => {
                    await delay(200);
                    actions.success(t, {async: true});
                }
            };

            taskQueue.register(QUEUE, "rfc004-task-b" as any, syncExec);
            taskQueue.register(QUEUE, "rfc004-task", asyncExec);

            await messageQueue.addMessages(QUEUE, [syncTask, asyncTask]);
            await taskHandler.startConsumingTasks(QUEUE);
            await delay(2000);

            const storedSync = await databaseAdapter.getTasksByIds(["j2-sync"]);
            const storedAsync = await databaseAdapter.getTasksByIds(["j2-async"]);

            expect(storedSync[0].execution_result).toEqual({sync: true});
            expect(storedAsync[0].execution_result).toEqual({async: true});

            await asyncTaskManager.shutdown();
            await messageQueue.shutdown();
        }, 10000);

        it("J3: rejected promise frees slot", async () => {
            const mgr = makeManager({maxTasks: 1});

            const rejectPromise = delay(30).then(() => { throw new Error("reject"); });
            mgr.handoffTask(makeTask({id: "j3-reject"}), rejectPromise);

            expect(mgr.canAcceptTask()).toBe(false);

            await delay(100);

            expect(mgr.canAcceptTask()).toBe(true);
            expect(mgr.getMetrics().activeTaskCount).toBe(0);

            await mgr.shutdown();
        });
    });
});
