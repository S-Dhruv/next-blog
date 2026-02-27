import {AsyncLocalStorage} from "node:async_hooks";
import {Logger} from "@supergrowthai/utils/server";

export type LogStore = Record<string, string>;

const als = new AsyncLocalStorage<LogStore>();

// Wire ALS into Logger's context provider — single registration at module load
Logger.setContextProvider(() => als.getStore());

/**
 * Run a function within an ALS scope carrying the given log context store.
 * The store is available via getLogContext() inside the callback and cleared after.
 */
export function runWithLogContext<T>(store: LogStore, fn: () => T): T {
    return als.run(store, fn);
}

/**
 * Read the current ALS log context store (undefined outside a runWithLogContext scope).
 */
export function getLogContext(): LogStore | undefined {
    return als.getStore();
}
