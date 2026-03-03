(() => {
    const normalizeParams = (raw, opName) => {
        if (raw === undefined) {
            return [];
        }

        if (!Array.isArray(raw)) {
            throw new TypeError(`${opName} params must be an array`);
        }

        return raw;
    };

    const normalizeDbCall = (input, params, paramsProvided, opName) => {
        if (typeof input === "string") {
            return { sql: input, params: normalizeParams(paramsProvided ? params : [], opName) };
        }

        if (typeof input === "object" && input !== null) {
            let resolved = input;
            if (typeof resolved.toSQL === "function") {
                resolved = resolved.toSQL();
            }

            if (typeof resolved === "object" && resolved !== null && typeof resolved.sql === "string") {
                const resolvedParams = paramsProvided ? params : resolved.params;
                return { sql: resolved.sql, params: normalizeParams(resolvedParams, opName) };
            }
        }

        throw new TypeError(
            `${opName} expects SQL input as string, { sql, params }, or object with toSQL()`
        );
    };

    const coreOps = globalThis.Deno?.core?.ops;
    if (!coreOps) {
        throw new Error("plts runtime bootstrap failed: Deno core ops are unavailable");
    }

    const ops = {
        dbQuery(input, params, readOnly = false, paramsProvided = false) {
            const call = normalizeDbCall(input, params, paramsProvided, "db.query");
            return coreOps.op_plts_db_query(call.sql, call.params, readOnly);
        },
        dbExec(input, params, readOnly = false, paramsProvided = false) {
            const call = normalizeDbCall(input, params, paramsProvided, "db.exec");
            return coreOps.op_plts_db_exec(call.sql, call.params, readOnly);
        },
    };

    Object.defineProperty(globalThis, "__plts_internal_ops", {
        value: Object.freeze(ops),
        configurable: false,
        enumerable: false,
        writable: false,
    });

    const stripGlobal = (key) => {
        try {
            delete globalThis[key];
        } catch (_err) {
            Object.defineProperty(globalThis, key, {
                value: undefined,
                configurable: true,
                enumerable: false,
                writable: false,
            });
        }
    };

    // stripGlobal("Deno");
    stripGlobal("fetch");
    stripGlobal("Request");
    stripGlobal("Response");
    stripGlobal("Headers");
    stripGlobal("WebSocket");
})();
