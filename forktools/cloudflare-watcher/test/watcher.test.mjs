import test from "node:test";
import assert from "node:assert/strict";
import worker, { decide, runWatch } from "../src/index.js";

const NOW = Date.parse("2026-11-03T12:00:00Z");
const ENV = {
  GH_TOKEN: "test-token",
  RETRY_COOLDOWN_SECONDS: "300",
};

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function makeFetch({
  assetId = "222",
  manifestAssetId = "111",
  runs = [],
  assetPresent = true,
  rollingMissing = false,
  dispatchStatus = 204,
} = {}) {
  const calls = [];
  const fetchImpl = async (url, options = {}) => {
    const href = String(url);
    calls.push({ url: href, options });

    if (href.endsWith("/repos/URenko/e-hentai-db/releases/tags/nightly")) {
      return json({
        assets: assetPresent
          ? [
              {
                name: "e-hentai.db.zstd",
                id: Number(assetId),
                state: "uploaded",
                updated_at: "2026-11-03T11:52:06Z",
              },
            ]
          : [],
      });
    }

    if (href.endsWith("/repos/fanout96/e-hentai-db/releases/tags/rolling")) {
      if (rollingMissing) {
        return json({ message: "Not Found" }, 404);
      }
      return json({
        assets: [
          {
            name: "manifest.json",
            url: "https://api.github.com/assets/manifest",
          },
        ],
      });
    }

    if (href === "https://api.github.com/assets/manifest") {
      return json({ upstream_asset_id: manifestAssetId });
    }

    if (href.includes("/actions/workflows/fork-delta-sync.yml/runs")) {
      return json({ workflow_runs: runs });
    }

    if (href.endsWith("/repos/fanout96/e-hentai-db/dispatches")) {
      return new Response(null, { status: dispatchStatus });
    }

    throw new Error(`unexpected fetch: ${href}`);
  };
  return { calls, fetchImpl };
}

function runAt(time, status = "completed", conclusion = "success") {
  return {
    id: 100,
    status,
    conclusion,
    created_at: new Date(time).toISOString(),
  };
}

test("decide: unchanged when manifest has the upstream asset id", () => {
  const result = decide({
    upstreamAssetId: "222",
    manifestAssetId: "222",
    runs: [],
    now: NOW,
  });
  assert.equal(result.action, "none");
  assert.equal(result.reason, "already published");
});

test("decide: waits while a workflow run is active", () => {
  const result = decide({
    upstreamAssetId: "222",
    manifestAssetId: "111",
    runs: [runAt(NOW - 1000, "in_progress")],
    now: NOW,
  });
  assert.equal(result.action, "wait");
  assert.equal(result.reason, "workflow run active");
});

test("decide: ignores a queued run that has been stuck for days", () => {
  const result = decide({
    upstreamAssetId: "222",
    manifestAssetId: "111",
    runs: [runAt(NOW - 3 * 24 * 60 * 60 * 1000, "queued")],
    now: NOW,
  });
  assert.equal(result.action, "dispatch");
});

test("decide: ignores an in-progress run past the job timeout", () => {
  const result = decide({
    upstreamAssetId: "222",
    manifestAssetId: "111",
    runs: [runAt(NOW - 181 * 60 * 1000, "in_progress")],
    now: NOW,
  });
  assert.equal(result.action, "dispatch");
});

test("decide: waits during retry cooldown after a failed run", () => {
  const result = decide({
    upstreamAssetId: "222",
    manifestAssetId: "111",
    runs: [runAt(NOW - 60_000, "completed", "failure")],
    now: NOW,
  });
  assert.equal(result.action, "wait");
  assert.equal(result.reason, "retry cooldown");
  assert.equal(result.runConclusion, "failure");
});

test("decide: dispatches when no active run and cooldown passed", () => {
  const result = decide({
    upstreamAssetId: "222",
    manifestAssetId: "111",
    runs: [runAt(NOW - 600_000, "completed", "failure")],
    now: NOW,
  });
  assert.equal(result.action, "dispatch");
});

test("runWatch: no dispatch when manifest already matches upstream", async () => {
  const { calls, fetchImpl } = makeFetch({
    assetId: "222",
    manifestAssetId: "222",
  });
  const result = await runWatch(ENV, { fetchImpl, now: () => NOW });
  assert.equal(result.action, "none");
  assert.equal(
    calls.some((call) => call.url.includes("/actions/workflows/")),
    false,
  );
  assert.equal(
    calls.some((call) => call.url.endsWith("/dispatches")),
    false,
  );
});

test("runWatch: dispatches with upstream asset metadata", async () => {
  const { calls, fetchImpl } = makeFetch({
    assetId: "222",
    manifestAssetId: "111",
    runs: [],
  });
  const result = await runWatch(ENV, { fetchImpl, now: () => NOW });
  assert.equal(result.action, "dispatch");
  assert.equal(result.dispatched, true);

  const dispatch = calls.find((call) => call.url.endsWith("/dispatches"));
  assert.ok(dispatch);
  assert.equal(dispatch.options.method, "POST");
  assert.equal(
    dispatch.options.headers.Authorization,
    "Bearer test-token",
  );

  const body = JSON.parse(dispatch.options.body);
  assert.equal(body.event_type, "fork-delta-sync");
  assert.equal(body.client_payload.upstream_asset_id, "222");
  assert.equal(
    body.client_payload.upstream_asset_updated_at,
    "2026-11-03T11:52:06Z",
  );
});

test("runWatch: waits for an active run instead of cancelling it", async () => {
  const { calls, fetchImpl } = makeFetch({
    assetId: "222",
    manifestAssetId: "111",
    runs: [runAt(NOW - 1000, "queued")],
  });
  const result = await runWatch(ENV, { fetchImpl, now: () => NOW });
  assert.equal(result.action, "wait");
  assert.equal(result.reason, "workflow run active");
  assert.equal(
    calls.some((call) => call.url.endsWith("/dispatches")),
    false,
  );
});

test("runWatch: dispatches despite a stale queued run", async () => {
  const { calls, fetchImpl } = makeFetch({
    assetId: "222",
    manifestAssetId: "111",
    runs: [runAt(NOW - 3 * 24 * 60 * 60 * 1000, "queued")],
  });
  const result = await runWatch(ENV, { fetchImpl, now: () => NOW });
  assert.equal(result.action, "dispatch");
  assert.equal(
    calls.some((call) => call.url.endsWith("/dispatches")),
    true,
  );
});

test("runWatch: retries after cooldown following a failed run", async () => {
  const { calls, fetchImpl } = makeFetch({
    assetId: "222",
    manifestAssetId: "111",
    runs: [runAt(NOW - 301_000, "completed", "failure")],
  });
  const result = await runWatch(ENV, { fetchImpl, now: () => NOW });
  assert.equal(result.action, "dispatch");
  assert.equal(
    calls.some((call) => call.url.endsWith("/dispatches")),
    true,
  );
});

test("runWatch: dry run returns dispatch decision without sending it", async () => {
  const { calls, fetchImpl } = makeFetch({
    assetId: "222",
    manifestAssetId: "111",
    runs: [],
  });
  const result = await runWatch(
    { ...ENV, DRY_RUN: "true" },
    { fetchImpl, now: () => NOW },
  );
  assert.equal(result.action, "dispatch");
  assert.equal(result.dryRun, true);
  assert.equal(
    calls.some((call) => call.url.endsWith("/dispatches")),
    false,
  );
});

test("runWatch: waits when upstream has no nightly asset", async () => {
  const { calls, fetchImpl } = makeFetch({ assetPresent: false });
  const result = await runWatch(ENV, { fetchImpl, now: () => NOW });
  assert.equal(result.action, "wait");
  assert.equal(result.reason, "upstream asset missing");
  assert.equal(calls.length, 1);
});

test("runWatch: dispatches when the rolling manifest is missing", async () => {
  const { calls, fetchImpl } = makeFetch({
    assetId: "222",
    rollingMissing: true,
    runs: [],
  });
  const result = await runWatch(ENV, { fetchImpl, now: () => NOW });
  assert.equal(result.action, "dispatch");
  assert.equal(result.manifestAssetId, null);
  assert.equal(
    calls.some((call) => call.url.endsWith("/dispatches")),
    true,
  );
});

test("worker: health endpoint reports configuration without secrets", async () => {
  const response = await worker.fetch(
    new Request("https://worker.test/health"),
    { GH_TOKEN: "token", WATCHER_SECRET: "secret", DRY_RUN: "true" },
  );
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), {
    ok: true,
    hasGhToken: true,
    hasUpstreamToken: false,
    hasWatcherSecret: true,
    dryRun: true,
  });
});

test("worker: manual run requires the configured secret", async () => {
  const response = await worker.fetch(
    new Request("https://worker.test/run", { method: "POST" }),
    {},
  );
  assert.equal(response.status, 401);
});

test("worker: dry-run check requires the configured secret", async () => {
  const response = await worker.fetch(
    new Request("https://worker.test/check"),
    {},
  );
  assert.equal(response.status, 401);
});

test("worker: authorized check reports a missing GitHub token", async () => {
  const response = await worker.fetch(
    new Request("https://worker.test/check", {
      headers: { Authorization: "Bearer secret" },
    }),
    { WATCHER_SECRET: "secret" },
  );
  assert.equal(response.status, 500);
  const body = await response.json();
  assert.match(body.error, /GH_TOKEN is required/);
});

test("worker: unknown routes return 404", async () => {
  const response = await worker.fetch(
    new Request("https://worker.test/nope"),
    {},
  );
  assert.equal(response.status, 404);
});
