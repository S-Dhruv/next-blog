/**
 * RFC-002: Flow Orchestration Types
 *
 * Fan-out/fan-in flow orchestration for task queues.
 * An executor calls actions.startFlow(steps, config) to dispatch N parallel
 * step tasks, a barrier tracks their completion, and a join task is dispatched
 * when all steps finish.
 */

/** Metadata attached to step/join/timeout tasks via metadata.flow_meta */
export interface FlowMeta {
    /** Unique identifier for this flow instance */
    flow_id: string;
    /** Index of this step within the flow (0-based) */
    step_index: number;
    /** Total number of steps in the flow */
    total_steps: number;
    /** Join task configuration — dispatched when barrier is met */
    join: {
        type: string;
        queue_id: string;
    };
    /** What happens when a step fails */
    failure_policy: 'continue' | 'abort';
    /** True on the join task itself */
    is_join?: boolean;
    /** True on timeout sentinel tasks */
    is_timeout?: boolean;
    /** Entity binding for flow-level projection */
    entity?: { id: string; type: string };
}

/** Result of a single flow step, stored in the barrier */
export interface FlowStepResult {
    step_index: number;
    status: 'success' | 'fail';
    result?: unknown;
    error?: string;
}

/** Aggregated results delivered to the join task via payload.flow_results */
export interface FlowResults {
    flow_id: string;
    steps: FlowStepResult[];
    /** True if the flow was aborted due to failure_policy: 'abort' */
    aborted?: boolean;
    /** True if the flow timed out before all steps completed */
    timed_out?: boolean;
}

/** A single step in a flow */
export interface FlowStep {
    type: string;
    queue_id: string;
    payload: unknown;
    /** Optional entity binding for this step */
    entity?: { id: string; type: string };
}

/** Configuration for startFlow */
export interface StartFlowConfig {
    /** Join task configuration */
    join: {
        type: string;
        queue_id: string;
    };
    /** What happens when a step fails: 'continue' (default) or 'abort' */
    failure_policy?: 'continue' | 'abort';
    /** Optional timeout in ms — dispatches a sentinel task */
    timeout_ms?: number;
    /** Optional entity binding for flow-level projection (task_id = flow_id) */
    entity?: { id: string; type: string };
}

/** Input to actions.startFlow() */
export interface StartFlowInput {
    steps: FlowStep[];
    config: StartFlowConfig;
}
