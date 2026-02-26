/**
 * Regression tests for two immediate-mode bugs:
 *
 * BUG-1: addTasks() entity projection overwrites terminal status.
 *   In addTasks(), the 'scheduled' projection write happened AFTER addMessages().
 *   When a consumer processes the task synchronously during addMessages()
 *   (ImmediateQueue), postProcessTasks writes 'executed' first, then addTasks
 *   overwrites it with 'scheduled'.
 *
 * BUG-2: Retried tasks with store_on_failure get stuck in immediate mode.
 *   When a task with an ID fails and has retries remaining, postProcessTasks
 *   called updateTasksForRetry() which only persists to DB. Without
 *   processMatureTasks() running, the retry was never re-enqueued to MQ.
 *
 * Fix: BUG-1 — projection writes moved before addMessages() in each path.
 * Fix: BUG-2 — retry tasks are now re-enqueued to MQ after DB persist,
 *       with DB status='processing' to prevent double-execution via mature poller.
 */
import {describe, expect, it, mock} from "bun:test";
import {TaskHandler} from "../core/TaskHandler.js";
import {TaskQueuesManager} from "../core/TaskQueuesManager.js";
import {InMemoryAdapter, CronTask} from "../adapters";
import {ImmediateQueue, InMemoryQueue, QueueName} from "@supergrowthai/mq";
import type {BaseMessage} from "@supergrowthai/mq";
import {MemoryCacheProvider} from "memoose-js";
import type {ISingleTaskNonParallel} from "../core/base/interfaces.js";
import type {
    IEntityProjectionProvider,
    EntityTaskProjection,
} from "../core/entity/IEntityProjectionProvider.js";

declare module "@supergrowthai/mq" {
    interface QueueRegistry {
        "test-immediate-queue": "test-immediate-queue";
    }
    interface MessagePayloadRegistry {
        "immediate-task": { data: string };
    }
}

const queueName = "test-immediate-queue" as QueueName;

// =====================================================================
// Test helpers
// =====================================================================

function makeEntityTask(
    id: string,
    entityId: string,
    entityType: string = "order",
    overrides: Partial<CronTask<string>> = {}
): CronTask<string> {
    return {
        id,
        type: "immediate-task" as any,
        queue_id: queueName,
        payload: {data: "test"},
        execute_at: new Date(),
        status: "scheduled",
        retries: 0,
        created_at: new Date(),
        updated_at: new Date(),
        entity: {id: entityId, type: entityType},
        ...overrides,
    } as CronTask<string>;
}

/**
 * Mock projection provider that records every upsert call in order.
 * Captures a clone of projections at each call site to preserve the
 * temporal ordering — essential for verifying BUG-1 (projection sequence).
 */
function createMockProvider(): IEntityProjectionProvider<string> & {
    calls: EntityTaskProjection<string>[][];
    allProjectionsFlat: () => EntityTaskProjection<string>[];
    lastProjectionFor: (taskId: string) => EntityTaskProjection<string> | undefined;
    statusSequenceFor: (entityId: string) => string[];
} {
    const calls: EntityTaskProjection<string>[][] = [];
    return {
        calls,
        upsertProjections: mock(async (entries: EntityTaskProjection<string>[]) => {
            calls.push([...entries]); // clone to capture point-in-time
        }),
        allProjectionsFlat() {
            return calls.flat();
        },
        lastProjectionFor(taskId: string) {
            const all = calls.flat().filter(p => p.task_id === taskId);
            return all[all.length - 1];
        },
        statusSequenceFor(entityId: string) {
            return calls.flat()
                .filter(p => p.entity_id === entityId)
                .map(p => p.status);
        },
    };
}

/**
 * Creates a stack using the official ImmediateQueue — processes messages
 * synchronously inside addMessages(), reproducing the real-world immediate mode.
 */
function createImmediateStack(provider?: IEntityProjectionProvider<string>) {
    const databaseAdapter = new InMemoryAdapter();
    const messageQueue = new ImmediateQueue<string>();
    const cacheProvider = new MemoryCacheProvider();
    const taskQueue = new TaskQueuesManager<string>(messageQueue);

    messageQueue.register(queueName);

    const taskHandler = new TaskHandler<string>(
        messageQueue,
        taskQueue,
        databaseAdapter,
        cacheProvider,
        undefined,
        undefined,
        {
            entityProjection: provider,
        }
    );

    return {databaseAdapter, messageQueue, cacheProvider, taskQueue, taskHandler};
}

/**
 * Creates a stack using InMemoryQueue — async/polling mode, used for
 * isolated unit tests that call postProcessTasks() directly.
 */
function createStandardStack(provider?: IEntityProjectionProvider<string>) {
    const databaseAdapter = new InMemoryAdapter();
    const messageQueue = new InMemoryQueue();
    const cacheProvider = new MemoryCacheProvider();
    const taskQueue = new TaskQueuesManager<string>(messageQueue);

    messageQueue.register(queueName);

    const taskHandler = new TaskHandler<string>(
        messageQueue,
        taskQueue,
        databaseAdapter,
        cacheProvider,
        undefined,
        undefined,
        {
            entityProjection: provider,
        }
    );

    return {databaseAdapter, messageQueue, cacheProvider, taskQueue, taskHandler};
}


// =====================================================================
// BUG-1: addTasks() 'scheduled' projection overwrites terminal status
// =====================================================================

describe("BUG-1: addTasks projection overwrites terminal status in immediate mode", () => {

    it("REGRESSION: synchronous success → last projection must be 'executed', not 'scheduled'", async () => {
        const provider = createMockProvider();
        const {taskHandler, taskQueue, databaseAdapter} = createImmediateStack(provider);

        const executor: ISingleTaskNonParallel<string, "immediate-task"> = {
            multiple: false,
            parallel: false,
            store_on_failure: true,
            default_retries: 0,
            onTask: async (task, actions) => {
                actions.success(task, {result: "done"});
            },
        };
        taskQueue.register(queueName, "immediate-task", executor);

        // Consumer MUST be registered before addTasks — ImmediateQueue requires it
        await taskHandler.startConsumingTasks(queueName);

        const taskId = databaseAdapter.generateId();
        const task = makeEntityTask(taskId, "order-1");

        // Full pipeline: addTasks → addMessages → sync execution → postProcessTasks
        await taskHandler.addTasks([task]);

        // Verify projection sequence ends with terminal status
        const statuses = provider.statusSequenceFor("order-1");
        expect(statuses.length).toBeGreaterThanOrEqual(2);
        expect(statuses[statuses.length - 1]).toBe("executed");

        // BUG manifests as: [..., 'executed', 'scheduled'] — 'scheduled' is last
        // Fix ensures: 'scheduled' is written BEFORE addMessages, so terminal wins
    });

    it("REGRESSION: 'scheduled' must never appear after terminal 'executed' in projection sequence", async () => {
        const provider = createMockProvider();
        const {taskHandler, taskQueue, databaseAdapter} = createImmediateStack(provider);

        const executor: ISingleTaskNonParallel<string, "immediate-task"> = {
            multiple: false,
            parallel: false,
            store_on_failure: true,
            default_retries: 0,
            onTask: async (task, actions) => {
                actions.success(task);
            },
        };
        taskQueue.register(queueName, "immediate-task", executor);

        await taskHandler.startConsumingTasks(queueName);

        const taskId = databaseAdapter.generateId();
        await taskHandler.addTasks([makeEntityTask(taskId, "order-2")]);

        const statuses = provider.statusSequenceFor("order-2");

        // Find the terminal status position
        const executedIndex = statuses.indexOf('executed');
        expect(executedIndex).toBeGreaterThanOrEqual(0); // must have executed

        // Nothing should appear after the terminal projection
        const afterTerminal = statuses.slice(executedIndex + 1);
        expect(afterTerminal).toEqual([]);
    });

    it("REGRESSION: synchronous failure → last projection must be 'failed', not 'scheduled'", async () => {
        const provider = createMockProvider();
        const {taskHandler, taskQueue, databaseAdapter} = createImmediateStack(provider);

        const executor: ISingleTaskNonParallel<string, "immediate-task"> = {
            multiple: false,
            parallel: false,
            store_on_failure: true,
            default_retries: 0,
            onTask: async (task, actions) => {
                actions.fail(task, new Error("Boom"));
            },
        };
        taskQueue.register(queueName, "immediate-task", executor);

        await taskHandler.startConsumingTasks(queueName);

        const taskId = databaseAdapter.generateId();
        await taskHandler.addTasks([makeEntityTask(taskId, "order-3")]);

        const statuses = provider.statusSequenceFor("order-3");
        expect(statuses.length).toBeGreaterThanOrEqual(2);
        expect(statuses[statuses.length - 1]).toBe("failed");
    });
});


// =====================================================================
// BUG-2: Retried tasks get stuck in immediate mode
// =====================================================================

describe("BUG-2: Retried tasks stuck in immediate mode (no processMatureTasks)", () => {

    it("FIX: retry task persisted to DB with 'processing' AND re-enqueued to MQ", async () => {
        const provider = createMockProvider();
        const databaseAdapter = new InMemoryAdapter();
        const messageQueue = new InMemoryQueue();
        const cacheProvider = new MemoryCacheProvider();
        const taskQueue = new TaskQueuesManager<string>(messageQueue);

        messageQueue.register(queueName);

        // Spy on addMessages to verify MQ re-enqueue
        const originalAddMessages = messageQueue.addMessages.bind(messageQueue);
        const mqAddCalls: BaseMessage<string>[][] = [];
        messageQueue.addMessages = async (queueId: QueueName, messages: BaseMessage<string>[]) => {
            mqAddCalls.push([...messages]);
            return originalAddMessages(queueId, messages);
        };

        const taskHandler = new TaskHandler<string>(
            messageQueue,
            taskQueue,
            databaseAdapter,
            cacheProvider,
            undefined,
            undefined,
            {entityProjection: provider}
        );

        const executor: ISingleTaskNonParallel<string, "immediate-task"> = {
            multiple: false,
            parallel: false,
            store_on_failure: true,
            default_retries: 3,
            onTask: async (task, actions) => {
                actions.fail(task, new Error("Transient"));
            },
        };
        taskQueue.register(queueName, "immediate-task", executor);

        const taskId = databaseAdapter.generateId();
        const task = makeEntityTask(taskId, "order-stuck", "order", {
            retries: 3,
            retry_after: 100,
        });

        mqAddCalls.length = 0;

        await taskHandler.postProcessTasks({
            successTasks: [],
            failedTasks: [{
                ...task,
                execution_stats: {retry_count: 0, last_error: "Transient"},
            }],
            newTasks: [],
        });

        // 1. MQ received the retry task
        expect(mqAddCalls.length).toBe(1);
        expect(mqAddCalls[0][0].id).toBe(taskId);

        // 2. DB has status='processing' to block processMatureTasks
        const [retryInDb] = await databaseAdapter.getTasksByIds([taskId]);
        expect(retryInDb).toBeDefined();
        expect(retryInDb!.status).toBe("processing");
        expect(retryInDb!.execution_stats?.retry_count).toBe(1);
        expect(retryInDb!.execute_at.getTime()).toBeGreaterThan(Date.now());

        // 3. processMatureTasks CANNOT find this task — double-execution prevention
        const matureTasks = await databaseAdapter.getMatureTasks(Date.now() + 600_000);
        expect(matureTasks.find(t => t.id === taskId)).toBeUndefined();
    });

    it("retryable failure emits no terminal projection (retry is not a final failure)", async () => {
        const provider = createMockProvider();
        const {taskHandler, taskQueue, databaseAdapter} = createStandardStack(provider);

        const executor: ISingleTaskNonParallel<string, "immediate-task"> = {
            multiple: false,
            parallel: false,
            store_on_failure: true,
            default_retries: 2,
            onTask: async (task, actions) => {
                actions.fail(task, new Error("Transient"));
            },
        };
        taskQueue.register(queueName, "immediate-task", executor);

        const taskId = databaseAdapter.generateId();
        const task = makeEntityTask(taskId, "order-projection-check", "order", {
            retries: 2,
            retry_after: 100,
        });

        provider.calls.length = 0;

        // First failure with retries remaining — NOT terminal
        await taskHandler.postProcessTasks({
            successTasks: [],
            failedTasks: [{
                ...task,
                execution_stats: {retry_count: 0, last_error: "Transient"},
            }],
            newTasks: [],
        });

        // Must NOT emit 'executed' or 'failed' — the task still has retries
        const terminalProjections = provider.allProjectionsFlat()
            .filter(p => p.entity_id === "order-projection-check")
            .filter(p => p.status === 'executed' || p.status === 'failed');
        expect(terminalProjections.length).toBe(0);
    });

    it("FALLBACK: MQ failure leaves DB as 'processing' for stale recovery", async () => {
        const databaseAdapter = new InMemoryAdapter();
        const messageQueue = new InMemoryQueue();
        const cacheProvider = new MemoryCacheProvider();
        const taskQueue = new TaskQueuesManager<string>(messageQueue);

        messageQueue.register(queueName);

        // Force MQ failure after DB persist
        messageQueue.addMessages = async () => {
            throw new Error("MQ unavailable");
        };

        const taskHandler = new TaskHandler<string>(
            messageQueue,
            taskQueue,
            databaseAdapter,
            cacheProvider,
        );

        const executor: ISingleTaskNonParallel<string, "immediate-task"> = {
            multiple: false,
            parallel: false,
            store_on_failure: true,
            default_retries: 3,
            onTask: async (task, actions) => {
                actions.fail(task, new Error("Transient"));
            },
        };
        taskQueue.register(queueName, "immediate-task", executor);

        const taskId = databaseAdapter.generateId();
        const task = makeEntityTask(taskId, "order-fallback", "order", {
            retries: 3,
            retry_after: 100,
        });

        await taskHandler.postProcessTasks({
            successTasks: [],
            failedTasks: [{
                ...task,
                execution_stats: {retry_count: 0, last_error: "Transient"},
            }],
            newTasks: [],
        });

        // MQ failed → DB stays 'processing'. Stale recovery (2-day timeout in
        // getMatureTasks) will eventually reset it to 'scheduled'.
        const [retryTask] = await databaseAdapter.getTasksByIds([taskId]);
        expect(retryTask).toBeDefined();
        expect(retryTask!.status).toBe("processing");

        // processMatureTasks cannot pick it up immediately (double-exec prevention)
        const matureTasks = await databaseAdapter.getMatureTasks(Date.now() + 600_000);
        expect(matureTasks.find(t => t.id === taskId)).toBeUndefined();
    });

    it("E2E: fail → retry → succeed completes full lifecycle in immediate mode", async () => {
        const provider = createMockProvider();
        const {taskHandler, taskQueue, databaseAdapter} = createImmediateStack(provider);

        let executionCount = 0;
        const executor: ISingleTaskNonParallel<string, "immediate-task"> = {
            multiple: false,
            parallel: false,
            store_on_failure: true,
            default_retries: 2,
            onTask: async (task, actions) => {
                executionCount++;
                if (executionCount === 1) {
                    actions.fail(task, new Error("Transient failure"));
                } else {
                    actions.success(task, {attempt: executionCount});
                }
            },
        };
        taskQueue.register(queueName, "immediate-task", executor);

        await taskHandler.startConsumingTasks(queueName);

        const taskId = databaseAdapter.generateId();
        const task = makeEntityTask(taskId, "order-e2e", "order", {
            retries: 2,
            retry_after: 100,
        });

        // Full pipeline: addTasks → fail → postProcess → retry → MQ re-enqueue
        // → ImmediateQueue processes retry synchronously → succeed → postProcess
        await taskHandler.addTasks([task]);

        // Task must have been executed exactly twice (1 fail + 1 success)
        expect(executionCount).toBe(2);

        // Final projection must be 'executed' (not 'failed', not 'scheduled')
        const statuses = provider.statusSequenceFor("order-e2e");
        expect(statuses[statuses.length - 1]).toBe("executed");

        // DB should have the task marked as success
        const [finalTask] = await databaseAdapter.getTasksByIds([taskId]);
        expect(finalTask).toBeDefined();
        expect(finalTask!.status).toBe("executed");
    });
});