import {QueueName, TypedMessage} from "../types.js";

/** Framework metadata namespace on messages — keeps non-business fields off payload */
export interface MessageMetadata {
    /** User-provided key-value pairs for log correlation (RFC-005) */
    log_context?: Record<string, string>;
}

/**
 * Base message structure required by the message queue system
 */
export type BaseMessage<ID = any> = TypedMessage & {
    id?: ID;
    execute_at: Date;
    status: 'scheduled' | 'processing' | 'executed' | 'failed' | 'expired' | 'ignored';
    created_at: Date;
    updated_at: Date;
    queue_id: QueueName;

    retries?: number;
    retry_after?: number;
    expires_at?: Date;

    processing_started_at?: Date;
    force_store?: boolean;
    execution_stats?: Record<string, unknown>;

    /** Executor-defined override for Kinesis partition key */
    partition_key?: string;

    /** Framework metadata (log_context, future: tracing, priority, flow orchestration) */
    metadata?: MessageMetadata;
};

/**
 * Message processor function type - can optionally return a value
 */
export type MessageConsumer<ID, R = void> = (queueId: string, messages: BaseMessage<ID>[]) => Promise<R>;

