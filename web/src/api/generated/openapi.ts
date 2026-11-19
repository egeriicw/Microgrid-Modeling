// GENERATED from api/openapi.json by `npm run gen:api` (openapi-typescript). Do not edit by hand.
// `npm run check:api` (part of `npm test`) fails when this file is out of date with the contract.
export interface paths {
    "/api/v1/demo-compositions": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /** Predefined compositions */
        get: operations["demo_compositions_api_v1_demo_compositions_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/exposed-attributes/active": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /** Attributes users may filter on (active) */
        get: operations["get_active_api_v1_exposed_attributes_active_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/exposed-attributes/preview": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /** Pool preview: buildings matching each category */
        post: operations["preview_api_v1_exposed_attributes_preview_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/exposed-attributes/versions": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /** List stored versions (admin) */
        get: operations["list_versions_api_v1_exposed_attributes_versions_get"];
        put?: never;
        /** Save a new version (admin) */
        post: operations["create_version_api_v1_exposed_attributes_versions_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/exposed-attributes/versions/{version}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /** Read one version, with columns (admin) */
        get: operations["get_version_api_v1_exposed_attributes_versions__version__get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/exposed-attributes/versions/{version}/activate": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Make a version active (admin)
         * @description Activates a stored version, or 0 for the packaged starter set. The body is optional: send `expected_active_version` (the version you believe is active now) to fail with **409** instead of silently replacing another admin's activation; without it the newest activation wins.
         */
        post: operations["activate_version_api_v1_exposed_attributes_versions__version__activate_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/health": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /** Liveness */
        get: operations["health_api_v1_health_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/ready": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /** Readiness */
        get: operations["ready_api_v1_ready_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/runs": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /** List runs (own runs; admin sees all) */
        get: operations["list_runs_api_v1_runs_get"];
        put?: never;
        /** Create and queue a run */
        post: operations["create_run_api_v1_runs_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/runs/{run_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /** Run detail including the result */
        get: operations["get_run_api_v1_runs__run_id__get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/runs/{run_id}/cancel": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /** Request cancellation */
        post: operations["cancel_api_v1_runs__run_id__cancel_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/runs/{run_id}/events": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /** Server-sent progress events (resume with Last-Event-ID) */
        get: operations["run_events_api_v1_runs__run_id__events_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/runs/{run_id}/results": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /** Full results document (JSON written by the worker; server paths removed) */
        get: operations["get_results_api_v1_runs__run_id__results_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
}
export type webhooks = Record<string, never>;
export interface components {
    schemas: {
        /** ActivateRequest */
        ActivateRequest: {
            /**
             * Expected Active Version
             * @description The version you believe is active now (0 = packaged). If another admin activated something else meanwhile, the request fails with 409 and nothing changes. Omit it for last-writer-wins.
             */
            expected_active_version?: number | null;
        };
        /** ActiveConfigurationOut */
        ActiveConfigurationOut: {
            /** Activated At */
            activated_at: string | null;
            /**
             * Attributes
             * @description Active attributes only, in display order
             */
            attributes: components["schemas"]["OfferedAttribute"][];
            /**
             * Source
             * @enum {string}
             */
            source: "packaged" | "database";
            /**
             * Version
             * @description 0 = the packaged starter set (nothing stored or activated yet)
             */
            version: number;
        };
        /** DemoCompositions */
        DemoCompositions: {
            /** Items */
            items: string[];
        };
        /**
         * ExposedAttributeIn
         * @description One entry of the exposed-attribute document (same fields as ``exposed_attributes.yaml``).
         */
        ExposedAttributeIn: {
            /**
             * Active
             * @default true
             */
            active: boolean;
            /**
             * Building Types
             * @default []
             */
            building_types: string[];
            /** Choices */
            choices?: string[] | null;
            /** Columns */
            columns: {
                [key: string]: string;
            };
            /**
             * Help
             * @default
             */
            help: string;
            /** Key */
            key: string;
            /**
             * Kind
             * @enum {string}
             */
            kind: "numeric" | "categorical" | "boolean";
            /** Label */
            label: string;
            /** Max */
            max?: number | null;
            /** Min */
            min?: number | null;
            /** Operators */
            operators: ("equals" | "in" | "range" | "not_null" | "is_null" | "is_true" | "is_false")[];
            /**
             * Order
             * @default 100
             */
            order: number;
        };
        /**
         * ExposedAttributesDocument
         * @description A full configuration document. ``version`` is the document format (always 1), not the stored version number.
         */
        ExposedAttributesDocument: {
            /** Attributes */
            attributes: components["schemas"]["ExposedAttributeIn"][];
            /**
             * Version
             * @constant
             */
            version: 1;
        };
        /** HealthOut */
        HealthOut: {
            /**
             * Status
             * @default ok
             */
            status: string;
        };
        /** HTTPValidationError */
        HTTPValidationError: {
            /** Detail */
            detail?: components["schemas"]["ValidationError"][];
        };
        /** OfferedAttribute */
        OfferedAttribute: {
            /** Building Types */
            building_types: string[];
            /** Choices */
            choices: string[] | null;
            /** Help */
            help: string;
            /** Key */
            key: string;
            /**
             * Kind
             * @enum {string}
             */
            kind: "numeric" | "categorical" | "boolean";
            /** Label */
            label: string;
            /** Max */
            max: number | null;
            /** Min */
            min: number | null;
            /** Operators */
            operators: ("equals" | "in" | "range" | "not_null" | "is_null" | "is_true" | "is_false")[];
            /** Order */
            order: number;
            /** Sources */
            sources: ("resstock" | "comstock")[];
        };
        /** PoolPreviewOut */
        PoolPreviewOut: {
            /** Matched */
            matched: number;
            /** Total */
            total: number;
            /** Warnings */
            warnings: string[];
        };
        /** PreviewOut */
        PreviewOut: {
            /**
             * Categories
             * @description Keyed "resstock/<category>" or "comstock/<category>"
             */
            categories: {
                [key: string]: components["schemas"]["PoolPreviewOut"];
            };
            /** Exposed Attributes Version */
            exposed_attributes_version: number;
            /** Min Pool Size */
            min_pool_size: number;
        };
        /**
         * PreviewRequest
         * @description A scenario config WITHOUT a `data` section (the server adds it) plus optional selections per category.
         */
        PreviewRequest: {
            /** Config */
            config: {
                [key: string]: unknown;
            };
            /** Filter Selections */
            filter_selections?: {
                [key: string]: components["schemas"]["Selection"][];
            } | null;
        };
        /** ReadyOut */
        ReadyOut: {
            /** Checks */
            checks: {
                [key: string]: string;
            };
            /** Ready */
            ready: boolean;
        };
        /**
         * RunCreate
         * @description Exactly one of ``config`` (a scenario WITHOUT a `data` section) or ``composition_id`` (a predefined one).
         */
        RunCreate: {
            /** Composition Id */
            composition_id?: string | null;
            /** Config */
            config?: {
                [key: string]: unknown;
            } | null;
            /** Filter Selections */
            filter_selections?: {
                [key: string]: components["schemas"]["Selection"][];
            } | null;
            /** Seeds */
            seeds?: {
                [key: string]: number;
            } | null;
        };
        /** RunDetail */
        RunDetail: {
            /** Cancel Requested */
            cancel_requested: boolean;
            /** Composition Snapshot */
            composition_snapshot: {
                [key: string]: unknown;
            };
            /**
             * Created At
             * Format: date-time
             */
            created_at: string;
            /** Effective Filters */
            effective_filters: {
                [key: string]: {
                    [key: string]: unknown;
                }[];
            };
            /** Engine Version */
            engine_version: string;
            /** Error Message */
            error_message: string | null;
            /** Filter Selections */
            filter_selections: {
                [key: string]: unknown;
            } | null;
            /** Finished At */
            finished_at: string | null;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Owner */
            owner: string | null;
            /** Pool Summary */
            pool_summary: {
                [key: string]: unknown;
            } | null;
            /** Progress */
            progress: {
                [key: string]: unknown;
            };
            /** Result */
            result: {
                [key: string]: unknown;
            } | null;
            /** Seeds */
            seeds: {
                [key: string]: unknown;
            };
            /** Started At */
            started_at: string | null;
            /**
             * Status
             * @enum {string}
             */
            status: "queued" | "running" | "succeeded" | "failed" | "cancelled";
        };
        /** RunList */
        RunList: {
            /** Items */
            items: components["schemas"]["RunOut"][];
            /** Limit */
            limit: number;
            /** Offset */
            offset: number;
            /** Total */
            total: number;
        };
        /** RunOut */
        RunOut: {
            /** Cancel Requested */
            cancel_requested: boolean;
            /**
             * Created At
             * Format: date-time
             */
            created_at: string;
            /** Engine Version */
            engine_version: string;
            /** Error Message */
            error_message: string | null;
            /** Finished At */
            finished_at: string | null;
            /**
             * Id
             * Format: uuid
             */
            id: string;
            /** Owner */
            owner: string | null;
            /** Progress */
            progress: {
                [key: string]: unknown;
            };
            /** Seeds */
            seeds: {
                [key: string]: unknown;
            };
            /** Started At */
            started_at: string | null;
            /**
             * Status
             * @enum {string}
             */
            status: "queued" | "running" | "succeeded" | "failed" | "cancelled";
        };
        /**
         * Selection
         * @description One user selection on an exposed attribute. There is deliberately no ``column`` field.
         */
        Selection: {
            /** Attribute */
            attribute: string;
            /** Max */
            max?: number | null;
            /** Max Inclusive */
            max_inclusive?: boolean | null;
            /** Min */
            min?: number | null;
            /** Min Inclusive */
            min_inclusive?: boolean | null;
            /**
             * Op
             * @enum {string}
             */
            op: "equals" | "in" | "range" | "not_null" | "is_null" | "is_true" | "is_false";
            /** Value */
            value?: boolean | number | string | null;
            /** Values */
            values?: (boolean | number | string)[] | null;
        };
        /** ValidationError */
        ValidationError: {
            /** Context */
            ctx?: Record<string, never>;
            /** Input */
            input?: unknown;
            /** Location */
            loc: (string | number)[];
            /** Message */
            msg: string;
            /** Error Type */
            type: string;
        };
        /** VersionCreate */
        VersionCreate: {
            /**
             * Activate
             * @default false
             */
            activate: boolean;
            /**
             * Base Version
             * @description The latest version number you started from (0 when none is stored). A stale value is a 409.
             */
            base_version: number;
            document: components["schemas"]["ExposedAttributesDocument"];
            /** Note */
            note?: string | null;
        };
        /** VersionDetail */
        VersionDetail: {
            /** Active */
            active: boolean;
            /** Based On */
            based_on: number | null;
            /** Created At */
            created_at: string | null;
            /**
             * Created By
             * @description None for version 0 (packaged)
             */
            created_by: string | null;
            document: components["schemas"]["ExposedAttributesDocument"];
            /** Note */
            note: string | null;
            /** Version */
            version: number;
        };
        /** VersionList */
        VersionList: {
            /** Active Version */
            active_version: number;
            /** Items */
            items: components["schemas"]["VersionOut"][];
            /** Latest Version */
            latest_version: number;
            /** Limit */
            limit: number;
            /** Offset */
            offset: number;
            /** Total */
            total: number;
        };
        /** VersionOut */
        VersionOut: {
            /** Active */
            active: boolean;
            /** Based On */
            based_on: number | null;
            /** Created At */
            created_at: string | null;
            /**
             * Created By
             * @description None for version 0 (packaged)
             */
            created_by: string | null;
            /** Note */
            note: string | null;
            /** Version */
            version: number;
        };
    };
    responses: never;
    parameters: never;
    requestBodies: never;
    headers: never;
    pathItems: never;
}
export type $defs = Record<string, never>;
export interface operations {
    demo_compositions_api_v1_demo_compositions_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["DemoCompositions"];
                };
            };
        };
    };
    get_active_api_v1_exposed_attributes_active_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ActiveConfigurationOut"];
                };
            };
        };
    };
    preview_api_v1_exposed_attributes_preview_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["PreviewRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["PreviewOut"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
        };
    };
    list_versions_api_v1_exposed_attributes_versions_get: {
        parameters: {
            query?: {
                limit?: number;
                offset?: number;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["VersionList"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
        };
    };
    create_version_api_v1_exposed_attributes_versions_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["VersionCreate"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["VersionOut"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
        };
    };
    get_version_api_v1_exposed_attributes_versions__version__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                /** @description Stored version number; 0 is the packaged starter set */
                version: number;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["VersionDetail"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
        };
    };
    activate_version_api_v1_exposed_attributes_versions__version__activate_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                /** @description Stored version number; 0 is the packaged starter set */
                version: number;
            };
            cookie?: never;
        };
        requestBody?: {
            content: {
                "application/json": components["schemas"]["ActivateRequest"] | null;
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["VersionOut"];
                };
            };
            /** @description expected_active_version is stale (another admin activated a version) */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
        };
    };
    health_api_v1_health_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HealthOut"];
                };
            };
        };
    };
    ready_api_v1_ready_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ReadyOut"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ReadyOut"];
                };
            };
        };
    };
    list_runs_api_v1_runs_get: {
        parameters: {
            query?: {
                limit?: number;
                offset?: number;
                status?: ("queued" | "running" | "succeeded" | "failed" | "cancelled") | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["RunList"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
        };
    };
    create_run_api_v1_runs_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["RunCreate"];
            };
        };
        responses: {
            /** @description Successful Response */
            202: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["RunOut"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
        };
    };
    get_run_api_v1_runs__run_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                run_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["RunDetail"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
        };
    };
    cancel_api_v1_runs__run_id__cancel_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                run_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["RunOut"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
        };
    };
    run_events_api_v1_runs__run_id__events_get: {
        parameters: {
            query?: never;
            header?: {
                "last-event-id"?: string | null;
            };
            path: {
                run_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": unknown;
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
        };
    };
    get_results_api_v1_runs__run_id__results_get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                run_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": unknown;
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
        };
    };
}
