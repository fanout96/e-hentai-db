const GITHUB_API = "https://api.github.com";
const UPSTREAM_REPO = "URenko/e-hentai-db";
const UPSTREAM_TAG = "nightly";
const UPSTREAM_ASSET = "e-hentai.db.zstd";
const FORK_REPO = "fanout96/e-hentai-db";
const ROLLING_TAG = "rolling";
const WORKFLOW_FILE = "fork-delta-sync.yml";
const DISPATCH_EVENT = "fork-delta-sync";
const MANIFEST_ASSET = "manifest.json";
const DEFAULT_RETRY_COOLDOWN_SECONDS = 300;
const DEFAULT_MAX_QUEUED_RUN_MINUTES = 30;
const DEFAULT_MAX_RUNNING_RUN_MINUTES = 180;
const ACTIVE_STATUSES = new Set([
  "queued",
  "in_progress",
  "requested",
  "waiting",
  "pending",
]);
const QUEUED_STATUSES = new Set([
  "queued",
  "requested",
  "waiting",
  "pending",
]);

function githubHeaders(token, extra = {}) {
  return {
    Accept: "application/vnd.github+json",
    Authorization: `Bearer ${token}`,
    "X-GitHub-Api-Version": "2022-11-28",
    "User-Agent": "ehdb-release-watcher",
    ...extra,
  };
}

async function githubJson(fetchImpl, url, token, options = {}) {
  const response = await fetchImpl(url, {
    ...options,
    headers: githubHeaders(token, options.headers),
  });
  if (!response.ok) {
    throw new Error(`GitHub API ${response.status} for ${url}`);
  }
  return response.json();
}

export async function fetchUpstreamAsset(fetchImpl, token) {
  const url = `${GITHUB_API}/repos/${UPSTREAM_REPO}/releases/tags/${UPSTREAM_TAG}`;
  const release = await githubJson(fetchImpl, url, token);
  const asset = (release.assets || []).find((item) => item.name === UPSTREAM_ASSET);
  if (!asset || (asset.state && asset.state !== "uploaded")) {
    return null;
  }
  return {
    id: String(asset.id),
    updatedAt: asset.updated_at || null,
  };
}

export async function fetchManifestAssetId(fetchImpl, token) {
  const releaseUrl = `${GITHUB_API}/repos/${FORK_REPO}/releases/tags/${ROLLING_TAG}`;
  const releaseResponse = await fetchImpl(releaseUrl, {
    headers: githubHeaders(token),
  });
  if (releaseResponse.status === 404) {
    return null;
  }
  if (!releaseResponse.ok) {
    throw new Error(`GitHub API ${releaseResponse.status} for ${releaseUrl}`);
  }
  const release = await releaseResponse.json();
  const asset = (release.assets || []).find((item) => item.name === MANIFEST_ASSET);
  if (!asset) {
    return null;
  }
  const assetResponse = await fetchImpl(asset.url, {
    headers: githubHeaders(token, { Accept: "application/octet-stream" }),
  });
  if (assetResponse.status === 404) {
    return null;
  }
  if (!assetResponse.ok) {
    throw new Error(`GitHub API ${assetResponse.status} for ${asset.url}`);
  }
  const manifest = await assetResponse.json();
  return manifest.upstream_asset_id ? String(manifest.upstream_asset_id) : null;
}

export async function fetchRecentRuns(fetchImpl, token) {
  const url = `${GITHUB_API}/repos/${FORK_REPO}/actions/workflows/${WORKFLOW_FILE}/runs?per_page=20`;
  const data = await githubJson(fetchImpl, url, token);
  return data.workflow_runs || [];
}

export function isActiveRun(
  run,
  now,
  {
    maxQueuedRunMs = DEFAULT_MAX_QUEUED_RUN_MINUTES * 60_000,
    maxRunningRunMs = DEFAULT_MAX_RUNNING_RUN_MINUTES * 60_000,
  } = {},
) {
  if (!ACTIVE_STATUSES.has(run.status)) {
    return false;
  }
  const started = Date.parse(run.run_started_at || run.created_at || "");
  if (!Number.isFinite(started)) {
    return false;
  }
  const age = now - started;
  if (QUEUED_STATUSES.has(run.status)) {
    return age <= maxQueuedRunMs;
  }
  return age <= maxRunningRunMs;
}

export function decide({
  upstreamAssetId,
  manifestAssetId,
  runs,
  now = Date.now(),
  retryCooldownMs = DEFAULT_RETRY_COOLDOWN_SECONDS * 1000,
  maxQueuedRunMs = DEFAULT_MAX_QUEUED_RUN_MINUTES * 60_000,
  maxRunningRunMs = DEFAULT_MAX_RUNNING_RUN_MINUTES * 60_000,
}) {
  if (!upstreamAssetId) {
    return { action: "wait", reason: "upstream asset missing" };
  }
  if (manifestAssetId && manifestAssetId === upstreamAssetId) {
    return { action: "none", reason: "already published" };
  }
  const active = runs.find((run) =>
    isActiveRun(run, now, { maxQueuedRunMs, maxRunningRunMs }),
  );
  if (active) {
    return {
      action: "wait",
      reason: "workflow run active",
      runId: active.id,
      runStatus: active.status,
    };
  }
  const newest = runs.reduce(
    (acc, run) => {
      const time = Date.parse(run.created_at || "");
      if (Number.isFinite(time) && time > acc.time) {
        return { time, run };
      }
      return acc;
    },
    { time: Number.NEGATIVE_INFINITY, run: null },
  );
  if (newest.run && now - newest.time < retryCooldownMs) {
    return {
      action: "wait",
      reason: "retry cooldown",
      runId: newest.run.id,
      runStatus: newest.run.status,
      runConclusion: newest.run.conclusion,
    };
  }
  return { action: "dispatch", reason: "upstream asset not published" };
}

async function dispatchWorkflow(fetchImpl, token, asset) {
  const url = `${GITHUB_API}/repos/${FORK_REPO}/dispatches`;
  const response = await fetchImpl(url, {
    method: "POST",
    headers: githubHeaders(token, { "Content-Type": "application/json" }),
    body: JSON.stringify({
      event_type: DISPATCH_EVENT,
      client_payload: {
        upstream_asset_id: asset.id,
        upstream_asset_updated_at: asset.updatedAt,
      },
    }),
  });
  if (response.status !== 204) {
    const body = await response.text().catch(() => "");
    throw new Error(`dispatch failed: ${response.status} ${body}`.trim());
  }
  return { status: response.status };
}

function positiveNumber(value, fallback) {
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? number : fallback;
}

function retryCooldownMs(env) {
  return (
    positiveNumber(env.RETRY_COOLDOWN_SECONDS, DEFAULT_RETRY_COOLDOWN_SECONDS) *
    1000
  );
}

function maxQueuedRunAgeMs(env) {
  return (
    positiveNumber(
      env.MAX_QUEUED_RUN_MINUTES,
      DEFAULT_MAX_QUEUED_RUN_MINUTES,
    ) * 60_000
  );
}

function maxRunningRunAgeMs(env) {
  return (
    positiveNumber(
      env.MAX_RUNNING_RUN_MINUTES,
      DEFAULT_MAX_RUNNING_RUN_MINUTES,
    ) * 60_000
  );
}

function isDryRun(env) {
  return env.DRY_RUN === "true" || env.DRY_RUN === "1";
}

function healthPayload(env) {
  return {
    ok: true,
    hasGhToken: Boolean(env.GH_TOKEN),
    hasUpstreamToken: Boolean(env.UPSTREAM_TOKEN),
    hasWatcherSecret: Boolean(env.WATCHER_SECRET),
    dryRun: isDryRun(env),
  };
}

function isAuthorized(request, env) {
  return (
    Boolean(env.WATCHER_SECRET) &&
    request.headers.get("Authorization") === `Bearer ${env.WATCHER_SECRET}`
  );
}

export async function runWatch(
  env,
  { fetchImpl = fetch, now = Date.now } = {},
) {
  const token = env.GH_TOKEN;
  if (!token) {
    throw new Error("GH_TOKEN is required");
  }
  const upstreamToken = env.UPSTREAM_TOKEN || token;
  const upstreamAsset = await fetchUpstreamAsset(fetchImpl, upstreamToken);
  if (!upstreamAsset) {
    return { action: "wait", reason: "upstream asset missing" };
  }

  const manifestAssetId = await fetchManifestAssetId(fetchImpl, token);
  if (manifestAssetId === upstreamAsset.id) {
    return {
      action: "none",
      reason: "already published",
      upstreamAssetId: upstreamAsset.id,
      manifestAssetId,
    };
  }

  const runs = await fetchRecentRuns(fetchImpl, token);
  const decision = decide({
    upstreamAssetId: upstreamAsset.id,
    manifestAssetId,
    runs,
    now: now(),
    retryCooldownMs: retryCooldownMs(env),
    maxQueuedRunMs: maxQueuedRunAgeMs(env),
    maxRunningRunMs: maxRunningRunAgeMs(env),
  });
  const result = {
    ...decision,
    upstreamAssetId: upstreamAsset.id,
    manifestAssetId,
  };
  if (decision.action !== "dispatch") {
    return result;
  }
  if (isDryRun(env)) {
    return { ...result, dryRun: true };
  }
  const dispatch = await dispatchWorkflow(fetchImpl, token, upstreamAsset);
  return { ...result, dispatched: true, dispatchStatus: dispatch.status };
}

export default {
  async scheduled(controller, env, _ctx) {
    console.log(
      JSON.stringify({
        source: "cron",
        event: "tick",
        scheduledTime: controller.scheduledTime,
      }),
    );
    try {
      const result = await runWatch(env);
      console.log(JSON.stringify({ source: "cron", ...result }));
    } catch (error) {
      console.error(JSON.stringify({ source: "cron", error: String(error) }));
    }
  },

  async fetch(request, env, _ctx) {
    const url = new URL(request.url);
    if (url.pathname === "/health") {
      return Response.json(healthPayload(env));
    }
    if (url.pathname === "/check") {
      if (!isAuthorized(request, env)) {
        return new Response("Unauthorized", { status: 401 });
      }
      try {
        const result = await runWatch({ ...env, DRY_RUN: "true" });
        return Response.json(result);
      } catch (error) {
        return Response.json({ error: String(error) }, { status: 500 });
      }
    }
    if (url.pathname !== "/run") {
      return new Response("Not found", { status: 404 });
    }
    if (request.method !== "POST") {
      return new Response("Method not allowed", { status: 405 });
    }
    if (!isAuthorized(request, env)) {
      return new Response("Unauthorized", { status: 401 });
    }
    try {
      const result = await runWatch(env);
      return Response.json(result);
    } catch (error) {
      return Response.json({ error: String(error) }, { status: 500 });
    }
  },
};
