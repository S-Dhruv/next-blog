import {describe, expect, it} from "bun:test";
import {CronTask, InMemoryAdapter} from "../adapters";
import {TaskStore} from "../core/TaskStore.js";
import type {QueueName} from "@supergrowthai/mq";
declare module "@supergrowthai/mq" {
    interface QueueRegistry {
        "adapter-test-queue": "adapter-test-queue";
    }

    interface MessagePayloadRegistry {
        "adapter-test-task": { input: string };
    }
}

const QUEUE: QueueName = "adapter-test-queue";

function makeTask(overrides: Partial<CronTask<string>> = {}): CronTask<string> {
    const id = `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
    return {
        id,
        type: "adapter-test-task",
        queue_id: QUEUE,
        payload: {input: "hello"},
        execute_at: new Date(),
        status: "scheduled",
        retries: 0,
        created_at: new Date(),
        updated_at: new Date(),
        processing_started_at: new Date(),
        ...overrides
    } as CronTask<string>;
}

/**
 * Spy adapter that wraps InMemoryAdapter and captures what addTasksToScheduled receives.
 * This lets us test what TaskStore's explicit field map actually passes through,
 * independent of InMemoryAdapter's spread behavior.
 */
class SpyAdapter extends InMemoryAdapter {
    public capturedTasks: CronTask<string>[] = [];

    async addTasksToScheduled(tasks: CronTask<string>[]): Promise<CronTask<string>[]> {
        this.capturedTasks = tasks;
        return super.addTasksToScheduled(tasks);
    }
}

describe("Adapter Consistency", () => {

    describe("partition_key preservation through TaskStore field map", () => {

        it("TaskStore.addTasksToScheduled passes partition_key to the adapter", async () => {
            // Bug: TaskStore.addTasksToScheduled uses explicit field enumeration
            // and omits partition_key. The spy captures what the adapter actually receives
            // AFTER TaskStore's transformation, so this fails if partition_key is dropped.
            const spy = new SpyAdapter();
            const store = new TaskStore<string>(spy);
            const task = makeTask({partition_key: "user-123"});

            await store.addTasksToScheduled([task]);

            // Check the transformed task that TaskStore passed to the adapter
            expect(spy.capturedTasks).toHaveLength(1);
            expect(spy.capturedTasks[0].partition_key).toBe("user-123");
        });

        it("TaskStore.addTasksToScheduled round-trips partition_key to storage", async () => {
            const adapter = new InMemoryAdapter();
            const store = new TaskStore<string>(adapter);
            const task = makeTask({partition_key: "tenant-456"});

            const [stored] = await store.addTasksToScheduled([task]);
            const [retrieved] = await adapter.getTasksByIds([stored.id!]);

            expect(retrieved.partition_key).toBe("tenant-456");
        });

        it("TaskStore.addTasksToScheduled does not drop partition_key when undefined", async () => {
            // Ensure we don't accidentally introduce errors for tasks without partition_key
            const spy = new SpyAdapter();
            const store = new TaskStore<string>(spy);
            const task = makeTask(); // no partition_key

            await store.addTasksToScheduled([task]);

            expect(spy.capturedTasks).toHaveLength(1);
            expect(spy.capturedTasks[0].partition_key).toBeUndefined();
        });
    });
});