/**
 * RFC-003: Entity Task Projection
 *
 * Automatic entity-task status projection so dashboards can track
 * task lifecycle without querying the internal tasks table.
 */

import type {CronTask} from "../../adapters/types.js";

export type EntityTaskProjectionStatus = 'scheduled' | 'processing' | 'executed' | 'failed';

export interface EntityTaskProjection<ID = any> {
    task_id: ID;
    entity_id: string;
    entity_type: string;
    task_type: string;
    queue_id: string;
    status: EntityTaskProjectionStatus;
    payload?: unknown;
    error?: string;
    result?: unknown;
    created_at: Date;
    updated_at: Date;
}

/**
 * Provider interface for persisting entity-task projections.
 * Implementations might write to a database table, cache, or external service.
 */
export interface IEntityProjectionProvider<ID = any> {
    upsertProjections(entries: EntityTaskProjection<ID>[]): Promise<void>;
}

/**
 * Configuration for entity projection behavior.
 */
export interface EntityProjectionConfig {
    /** Include task payload in projection (default: false for performance) */
    includePayload?: boolean;
}

/**
 * Sync entity projections to the provider. Non-fatal — logs and continues on error.
 */
export async function syncProjections<ID>(
    projections: EntityTaskProjection<ID>[],
    provider: IEntityProjectionProvider<ID> | undefined,
    logger: { error(msg: string): void }
): Promise<void> {
    if (projections.length === 0 || !provider) return;
    try {
        await provider.upsertProjections(projections);
    } catch (err) {
        logger.error(`[TQ] Entity projection sync failed (non-fatal): ${err}`);
    }
}

/**
 * Build a single projection entry from a CronTask and target status.
 * Returns null if the task has no entity binding.
 * Throws if entity is present but task has no ID — fail-fast for developer errors.
 */
export function buildProjection<ID>(
    task: CronTask<ID>,
    status: EntityTaskProjectionStatus,
    options?: {
        includePayload?: boolean;
        error?: string;
        result?: unknown;
    }
): EntityTaskProjection<ID> | null {
    if (!task.entity) return null;

    if (task.id == null) {
        throw new Error(
            `[TQ/RFC-003] Task with entity (${task.entity.type}:${task.entity.id}) has no task ID. ` +
            `Entity-bearing tasks must have an ID for projection keying. ` +
            `Set store_on_failure:true, force_store:true, or assign an ID before addTasks().`
        );
    }

    return {
        task_id: task.id,
        entity_id: task.entity.id,
        entity_type: task.entity.type,
        task_type: task.type,
        queue_id: task.queue_id,
        status,
        payload: options?.includePayload ? task.payload : undefined,
        error: options?.error,
        result: options?.result,
        created_at: task.created_at || new Date(),
        updated_at: new Date(),
    };
}