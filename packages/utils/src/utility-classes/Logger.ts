export enum LogLevel {
    DEBUG,
    INFO,
    WARN,
    ERROR
}

/**
 * Sanitize a context value: replace control chars, ], \x1b with _
 */
function sanitizeValue(value: string): string {
    return value.replace(/[\x00-\x1f\x7f\]]/g, '_');
}

/**
 * Format a context record into bracketed key:value pairs for log output.
 * Keys are sorted alphabetically for deterministic output.
 */
function formatContext(ctx: Record<string, string>): string {
    const keys = Object.keys(ctx).sort();
    if (keys.length === 0) return '';
    return keys.map(k => `[${k}:${sanitizeValue(ctx[k])}]`).join('');
}

type ContextProvider = (() => Record<string, string> | undefined) | null;

class Logger {
    private static contextProvider: ContextProvider = null;
    private readonly prefix: string;
    private logLevel: LogLevel;
    private readonly context: Record<string, string>;

    constructor(prefix: string, initialLogLevel: LogLevel = LogLevel.INFO, context?: Record<string, string>) {
        this.prefix = prefix;
        this.logLevel = initialLogLevel;
        this.context = context || {};
    }

    /**
     * Register a global context provider (typically backed by AsyncLocalStorage).
     * Pass null to clear.
     */
    static setContextProvider(fn: ContextProvider) {
        Logger.contextProvider = fn;
    }

    /**
     * Create a child logger with additional static context.
     * Child context keys override ALS context keys with the same name.
     */
    child(extraContext: Record<string, string>): Logger {
        return new Logger(this.prefix, this.logLevel, {...this.context, ...extraContext});
    }

    getLogLevel() {
        return this.logLevel;
    }

    setLogLevel(level: LogLevel) {
        this.logLevel = level;
    }

    /**
     * Resolve merged context: ALS provider (if any) + instance context (wins on collision).
     * Returns formatted string or empty string.
     */
    private resolveContext(): string {
        const alsCtx = Logger.contextProvider?.() || {};
        const merged = {...alsCtx, ...this.context};
        if (Object.keys(merged).length === 0) return '';
        return ' ' + formatContext(merged);
    }

    debug(message: string, ...args: any[]) {
        if (this.logLevel === LogLevel.DEBUG) {
            const ctx = this.resolveContext();
            console.debug(`[${this.prefix}]${ctx} ${message}`, ...args);
        }
    }

    info(message: string, ...args: any[]) {
        if (this.logLevel <= LogLevel.INFO) {
            const ctx = this.resolveContext();
            console.info(`[${this.prefix}]${ctx} ${message}`, ...args);
        }
    }

    error(message: string, ...args: any[]) {
        const ctx = this.resolveContext();
        console.error(`[${this.prefix}]${ctx} ${message}`, ...args);
    }

    warn(message: string, ...args: any[]) {
        if (this.logLevel <= LogLevel.WARN) {
            const ctx = this.resolveContext();
            console.warn(`[${this.prefix}]${ctx} ${message}`, ...args);
        }
    }

    time(message: string) {
        if (this.logLevel < LogLevel.INFO) {
            console.time(`[${this.prefix}] ${message}`)
        }
    }

    timeEnd(message: string) {
        if (this.logLevel < LogLevel.INFO) {
            console.timeEnd(`[${this.prefix}] ${message}`)
        }
    }
}


export default Logger
