import { describe, expect, it, spyOn } from "bun:test";
import Logger, { LogLevel } from "../utility-classes/Logger.js";

describe("Logger Context Provider", () => {
    it("contextProvider output appears in Logger.info formatted output", () => {
        const captured: string[] = [];
        const spy = spyOn(console, "info").mockImplementation((...args: any[]) => {
            captured.push(args.join(" "));
        });

        Logger.setContextProvider(() => ({ user_id: "abc", req_id: "xyz" }));
        const logger = new Logger("Test", LogLevel.INFO);
        logger.info("hello world");

        expect(captured).toHaveLength(1);
        expect(captured[0]).toContain("[Test]");
        expect(captured[0]).toContain("[user_id:abc]");
        expect(captured[0]).toContain("[req_id:xyz]");
        expect(captured[0]).toContain("hello world");

        Logger.setContextProvider(null);
        spy.mockRestore();
    });

    it("Logger without contextProvider produces prefix-only output", () => {
        const captured: string[] = [];
        const spy = spyOn(console, "info").mockImplementation((...args: any[]) => {
            captured.push(args.join(" "));
        });

        Logger.setContextProvider(null);
        const logger = new Logger("Clean", LogLevel.INFO);
        logger.info("no context");

        expect(captured[0]).toBe("[Clean] no context");

        spy.mockRestore();
    });

    it("child logger includes its context, parent does not", () => {
        const captured: string[] = [];
        const spy = spyOn(console, "info").mockImplementation((...args: any[]) => {
            captured.push(args.join(" "));
        });

        Logger.setContextProvider(null);
        const parent = new Logger("Parent", LogLevel.INFO);
        const child = parent.child({ tenant: "acme" });

        child.info("from child");
        parent.info("from parent");

        expect(captured[0]).toContain("[tenant:acme]");
        expect(captured[1]).not.toContain("[tenant:acme]");

        spy.mockRestore();
    });

    it("child instance context overrides ALS context for same key", () => {
        const captured: string[] = [];
        const spy = spyOn(console, "info").mockImplementation((...args: any[]) => {
            captured.push(args.join(" "));
        });

        Logger.setContextProvider(() => ({ user_id: "als_user", req_id: "als_req" }));
        const parent = new Logger("Test", LogLevel.INFO);
        const child = parent.child({ user_id: "child_user" });

        child.info("merged");

        expect(captured[0]).toContain("[user_id:child_user]");
        expect(captured[0]).toContain("[req_id:als_req]");
        expect(captured[0]).not.toContain("[user_id:als_user]");

        Logger.setContextProvider(null);
        spy.mockRestore();
    });

    it("contextProvider is not called for suppressed log levels", () => {
        let providerCallCount = 0;
        Logger.setContextProvider(() => {
            providerCallCount++;
            return { key: "val" };
        });

        const logger = new Logger("Test", LogLevel.ERROR);
        logger.info("suppressed");
        logger.debug("suppressed");

        expect(providerCallCount).toBe(0);

        Logger.setContextProvider(null);
    });

    it("error() always calls contextProvider regardless of log level", () => {
        let providerCallCount = 0;
        Logger.setContextProvider(() => {
            providerCallCount++;
            return { key: "val" };
        });

        const captured: string[] = [];
        const spy = spyOn(console, "error").mockImplementation((...args: any[]) => {
            captured.push(args.join(" "));
        });

        const logger = new Logger("Test", LogLevel.ERROR);
        logger.error("critical failure");

        expect(providerCallCount).toBe(1);
        expect(captured[0]).toContain("[key:val]");

        Logger.setContextProvider(null);
        spy.mockRestore();
    });

    it("time() and timeEnd() do not include context in labels", () => {
        Logger.setContextProvider(() => ({ user_id: "alice" }));

        const timeLabels: string[] = [];
        const timeSpy = spyOn(console, "time").mockImplementation((...args: any[]) => {
            timeLabels.push(args[0]);
        });
        const timeEndLabels: string[] = [];
        const timeEndSpy = spyOn(console, "timeEnd").mockImplementation((...args: any[]) => {
            timeEndLabels.push(args[0]);
        });

        const logger = new Logger("DB", LogLevel.DEBUG);
        logger.time("query");
        logger.timeEnd("query");

        expect(timeLabels[0]).toBe("[DB] query");
        expect(timeEndLabels[0]).toBe("[DB] query");
        expect(timeLabels[0]).not.toContain("[user_id:");

        Logger.setContextProvider(null);
        timeSpy.mockRestore();
        timeEndSpy.mockRestore();
    });

    it("formatContext sanitizes special characters in values", () => {
        const captured: string[] = [];
        const spy = spyOn(console, "info").mockImplementation((...args: any[]) => {
            captured.push(args.join(" "));
        });

        Logger.setContextProvider(() => ({
            safe: "normal",
            brackets: "val]ue",
            newline: "line1\nline2",
            ansi: "text\x1b[31mred",
        }));

        const logger = new Logger("Test", LogLevel.INFO);
        logger.info("check");

        expect(captured[0]).toContain("[safe:normal]");
        expect(captured[0]).toContain("[brackets:val_ue]");
        expect(captured[0]).toContain("[newline:line1_line2]");
        expect(captured[0]).toContain("[ansi:text_[31mred]");
        expect(captured[0]).not.toContain("\n");
        expect(captured[0]).not.toContain("\x1b");

        Logger.setContextProvider(null);
        spy.mockRestore();
    });
});
