'use strict';

/**
 * @fileoverview Maintenance task that hard-deletes escrow-read records whose
 * soft-delete retention window has elapsed (issue #31).
 *
 * Soft-deleting a record (see {@link module:services/escrowReadSoftDelete})
 * leaves a tombstoned `escrow_event_projection` row behind. Without a purge,
 * tombstones accumulate forever — the exact unbounded-growth problem the
 * idempotency purge job solves for `idempotency_keys`.
 *
 * This job runs the purge on a schedule through the shared job queue/worker
 * infrastructure, emits Prometheus counters, and exposes a manual trigger for
 * the admin API.
 *
 * ## Configuration
 * - `ESCROW_READ_SOFT_DELETE_RETENTION_DAYS` — restore/retention window (default 30).
 * - `ESCROW_READ_PURGE_BATCH_SIZE` — rows deleted per batch (default 500).
 * - `ESCROW_READ_PURGE_MAX_BATCHES` — batch cap per run (default 100).
 * - `ESCROW_READ_PURGE_INTERVAL_MS` — cadence between runs (default 6 h,
 *   min 1 min, max 7 days).
 *
 * ## Validation invariants
 * - `getIntervalMs()` is clamped to [MIN_INTERVAL_MS, MAX_INTERVAL_MS]; a
 *   misconfigured value (too small → scheduling storm, too large → silent
 *   purge stall) falls back to the safe default rather than the raw env value.
 * - `runEscrowReadPurge(job, options)` normalises `job` to a plain object so
 *   non-object callers cannot crash the handler. The injected `options` fields
 *   (`batchSize`, `maxBatches`, `now`) are individually range-checked and
 *   stripped of invalid values before being forwarded to the service layer.
 * - `schedulePurge(options)` clamps `delayMs` to [0, MAX_DELAY_MS], rejecting
 *   negative delays (which would trigger immediate cascading re-runs) and
 *   astronomical delays (which would silently stall the purge).
 *
 * @module jobs/escrowReadPurge
 */

const JobQueue = require('../workers/jobQueue');
const BackgroundWorker = require('../workers/worker');
const logger = require('../logger');
const { Counter } = require('prom-client');
const { getRegistry } = require('../metrics');
const {
  purgeExpiredSoftDeletes,
  getRetentionDays,
  getPurgeBatchSize,
  getPurgeMaxBatches,
} = require('../services/escrowReadSoftDelete');

/** @constant {string} */
const JOB_TYPE = 'escrow_read_purge';

/** @constant {number} Default purge cadence: 6 hours. */
const DEFAULT_INTERVAL_MS = 6 * 60 * 60 * 1000;

/**
 * Minimum allowed purge interval.
 *
 * Values below this floor would schedule the job so aggressively that the
 * worker could starve normal request traffic.
 *
 * @constant {number}
 */
const MIN_INTERVAL_MS = 60_000; // 1 minute

/**
 * Maximum allowed purge interval.
 *
 * Values above this ceiling would silently stall the purge: tombstones could
 * grow unbounded for days before the job fires. Seven days is chosen as the
 * outer safe bound — well beyond any reasonable maintenance window — so a
 * misconfigured large value is rejected rather than accepted silently.
 *
 * @constant {number}
 */
const MAX_INTERVAL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days

/**
 * Maximum allowed `delayMs` accepted by {@link schedulePurge}.
 *
 * Mirrors `MAX_INTERVAL_MS` so a scheduled delay cannot exceed one full purge
 * cycle. Values above this are clamped rather than rejected so callers can
 * pass `getIntervalMs()` directly without a separate guard.
 *
 * @constant {number}
 */
const MAX_DELAY_MS = MAX_INTERVAL_MS;

/**
 * Maximum rows the service layer accepts per batch (`MAX_PURGE_BATCH_SIZE` in
 * {@link module:services/escrowReadSoftDelete}). Duplicated here so the job
 * layer can clamp injected `batchSize` values without importing internal
 * service constants.
 *
 * @constant {number}
 */
const MAX_BATCH_SIZE = 10000;

/**
 * Maximum batch count the service layer accepts per run
 * (`MAX_PURGE_MAX_BATCHES` in {@link module:services/escrowReadSoftDelete}).
 * Duplicated here so the job layer can clamp injected `maxBatches` values
 * without importing internal service constants.
 *
 * @constant {number}
 */
const MAX_MAX_BATCHES = 1000;

/**
 * Registers a counter idempotently. Jest resets the module registry between
 * suites while `prom-client`'s registry is process-global, so a bare
 * `new Counter(...)` would throw "already registered" on the second load.
 *
 * @param {object} config - `prom-client` counter configuration.
 * @returns {import('prom-client').Counter} New or previously registered counter.
 */
function _counter(config) {
  const registry = getRegistry();
  const existing = registry.getSingleMetric(config.name);
  if (existing) {
    return existing;
  }
  return new Counter({ ...config, registers: [registry] });
}

const escrowReadPurgeRowsDeletedTotal = _counter({
  name: 'liquifact_escrow_read_purge_rows_deleted_total',
  help: 'Total escrow-read tombstones hard-deleted after their retention window',
});

const escrowReadPurgeRunsTotal = _counter({
  name: 'liquifact_escrow_read_purge_runs_total',
  help: 'Total escrow-read purge job runs by outcome',
  labelNames: ['status'],
});

/**
 * Reads the purge cadence from the environment and applies boundary checks.
 *
 * Clamping is applied in both directions:
 * - Values below `MIN_INTERVAL_MS` (< 1 min) would schedule the job so
 *   aggressively that it could starve normal traffic.
 * - Values above `MAX_INTERVAL_MS` (> 7 days) would silently stall the purge,
 *   allowing tombstones to accumulate beyond their intended retention window.
 *
 * Non-numeric, non-finite, and non-integer inputs (e.g. floats, `"abc"`,
 * `Infinity`) all fall back to the safe default.
 *
 * @returns {number} Interval in ms, clamped to
 *   [`MIN_INTERVAL_MS`, `MAX_INTERVAL_MS`]; default 6 h.
 */
function getIntervalMs() {
  const parsed = parseInt(process.env.ESCROW_READ_PURGE_INTERVAL_MS, 10);
  if (!Number.isFinite(parsed) || parsed < MIN_INTERVAL_MS) {
    return DEFAULT_INTERVAL_MS;
  }
  return Math.min(parsed, MAX_INTERVAL_MS);
}

/**
 * Normalises a raw `job` argument into a safe plain object.
 *
 * The worker framework passes a job envelope, but callers (admin endpoints,
 * tests) may pass non-object values. Normalising avoids a crash when the
 * handler accesses `job.id`.
 *
 * @param {unknown} raw - Raw `job` argument.
 * @returns {{ id?: string }} Safe job envelope.
 */
function _normaliseJob(raw) {
  if (raw !== null && typeof raw === 'object' && !Array.isArray(raw)) {
    return raw;
  }
  return {};
}

/**
 * Validates and sanitises the `options` forwarded to
 * {@link purgeExpiredSoftDeletes}.
 *
 * Only recognised fields are kept; unrecognised keys are dropped. Individual
 * fields are validated against their expected types and ranges:
 *
 * - `batchSize`  — must be a positive finite integer `number` (string values
 *   are rejected, not coerced); clamped to [1, `MAX_BATCH_SIZE`]; dropped if
 *   the type check fails.
 * - `maxBatches` — must be a positive finite integer `number` (string values
 *   are rejected, not coerced); clamped to [1, `MAX_MAX_BATCHES`]; dropped if
 *   the type check fails.
 * - `now`        — must be a finite number; dropped otherwise.
 * - `dbClient`   — any non-null object is forwarded as-is (test injection).
 *
 * Dropping an invalid field lets the service fall back to its own defaults
 * (from environment variables) rather than crashing or silently passing a
 * corrupt value into the DELETE query.
 *
 * @param {unknown} raw - Raw options argument.
 * @returns {object} Sanitised options object safe to pass to the service.
 */
function _sanitiseOptions(raw) {
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
    return {};
  }

  const safe = {};

  // dbClient — opaque Knex instance injected by tests; validate it is an object.
  if (raw.dbClient !== null && raw.dbClient !== undefined) {
    if (typeof raw.dbClient === 'object' && !Array.isArray(raw.dbClient)) {
      safe.dbClient = raw.dbClient;
    }
    // Non-object dbClient is silently dropped; service uses its own default.
  }

  // now — must be a finite number (epoch ms). Non-finite or non-number is dropped.
  if (raw.now !== undefined) {
    if (typeof raw.now === 'number' && Number.isFinite(raw.now)) {
      safe.now = raw.now;
    }
    // Invalid `now` is dropped; service uses Date.now() as its own default.
  }

  // batchSize — must be a positive finite integer (not a string).
  // Clamped to [1, MAX_BATCH_SIZE] so callers cannot exceed the service limit.
  if (raw.batchSize !== undefined) {
    if (typeof raw.batchSize === 'number') {
      const bs = raw.batchSize;
      if (Number.isFinite(bs) && Number.isInteger(bs) && bs > 0) {
        safe.batchSize = Math.min(bs, MAX_BATCH_SIZE);
      }
    }
    // Non-number or invalid batchSize is dropped; service uses getPurgeBatchSize().
  }

  // maxBatches — must be a positive finite integer (not a string).
  // Clamped to [1, MAX_MAX_BATCHES] so callers cannot exceed the service limit.
  if (raw.maxBatches !== undefined) {
    if (typeof raw.maxBatches === 'number') {
      const mb = raw.maxBatches;
      if (Number.isFinite(mb) && Number.isInteger(mb) && mb > 0) {
        safe.maxBatches = Math.min(mb, MAX_MAX_BATCHES);
      }
    }
    // Non-number or invalid maxBatches is dropped; service uses getPurgeMaxBatches().
  }

  return safe;
}

/**
 * Job handler: purges expired escrow-read tombstones and records metrics.
 *
 * `job` is normalised before accessing `job.id` so non-object callers cannot
 * crash the handler. `options` is sanitised before forwarding to the service
 * layer so invalid field values are replaced by the service's own safe
 * defaults rather than being passed through to the database query.
 *
 * @param {unknown} [job={}] - Job envelope from the queue (`id` used for logs).
 *   Non-object values are normalised to `{}`.
 * @param {object} [options={}] - Forwarded to
 *   {@link module:services/escrowReadSoftDelete.purgeExpiredSoftDeletes}
 *   (`dbClient`, `now`, `batchSize`, `maxBatches`) — used by tests.
 *   Invalid field values are individually stripped rather than rejected so
 *   the handler can always make forward progress.
 * @returns {Promise<object>} Purge summary plus `success: true`.
 * @throws {Error} Re-throws the underlying failure after recording metrics so
 *   the worker's retry policy applies.
 */
async function runEscrowReadPurge(job = {}, options = {}) {
  const safeJob = _normaliseJob(job);
  const safeOptions = _sanitiseOptions(options);
  const startedAt = Date.now();

  try {
    const summary = await purgeExpiredSoftDeletes(safeOptions);

    escrowReadPurgeRowsDeletedTotal.inc(summary.purged);
    escrowReadPurgeRunsTotal.inc({ status: 'success' });

    logger.info(
      {
        jobId: safeJob.id,
        purged: summary.purged,
        batches: summary.batches,
        cutoff: summary.cutoff,
        retentionDays: summary.retentionDays,
        maxBatchesReached: summary.maxBatchesReached,
        durationMs: Date.now() - startedAt,
      },
      'escrowReadPurge: run completed'
    );

    return { success: true, ...summary };
  } catch (error) {
    escrowReadPurgeRunsTotal.inc({ status: 'error' });
    logger.error(
      { jobId: safeJob.id, err: error.message, durationMs: Date.now() - startedAt },
      'escrowReadPurge: run failed'
    );
    throw error;
  }
}

const purgeQueue = new JobQueue();
const purgeWorker = new BackgroundWorker({
  jobQueue: purgeQueue,
  maxConcurrency: 1, // Serialised: concurrent purges would contend on the same rows.
  pollIntervalMs: 5000,
});

purgeWorker.registerHandler(JOB_TYPE, (job) => runEscrowReadPurge(job));

/**
 * Enqueues a purge run.
 *
 * `delayMs` is clamped to [0, `MAX_DELAY_MS`]:
 * - Negative values would trigger immediate execution regardless of intent,
 *   potentially creating a cascading storm of back-to-back purge runs.
 * - Values exceeding `MAX_DELAY_MS` (7 days) would silently stall the purge
 *   beyond any reasonable operational window.
 *
 * `NaN` and non-finite values fall back to the current interval so the queue
 * is always seeded with a valid delay.
 *
 * @param {object} [options={}]
 * @param {number} [options.delayMs=getIntervalMs()] - Desired delay in ms
 *   before execution. Clamped to [0, `MAX_DELAY_MS`].
 * @returns {string} Job ID.
 */
function schedulePurge(options = {}) {
  const raw = options.delayMs ?? getIntervalMs();
  let delayMs;

  if (typeof raw !== 'number' || !Number.isFinite(raw)) {
    // Non-numeric / NaN / Infinity: fall back to the configured interval.
    delayMs = getIntervalMs();
  } else {
    // Clamp: negative → 0 (run now); above ceiling → cap (don't stall forever).
    delayMs = Math.min(Math.max(0, Math.trunc(raw)), MAX_DELAY_MS);
  }

  const jobId = purgeQueue.enqueue(JOB_TYPE, {}, { delayMs });
  logger.debug({ jobId, delayMs }, 'escrowReadPurge: scheduled run');
  return jobId;
}

/**
 * Starts the worker and schedules the first run. Safe to call twice.
 *
 * @returns {void}
 */
function startPurgeWorker() {
  if (!purgeWorker.isRunning) {
    purgeWorker.start();
    schedulePurge();
    logger.info(
      { retentionDays: getRetentionDays(), intervalMs: getIntervalMs() },
      'escrowReadPurge: worker started'
    );
  }
}

/**
 * Stops the worker, allowing in-flight runs to finish.
 *
 * @param {number} [timeoutMs=10000] - Grace period.
 * @returns {Promise<void>}
 */
async function stopPurgeWorker(timeoutMs = 10000) {
  await purgeWorker.stop(timeoutMs);
  logger.info('escrowReadPurge: worker stopped');
}

/**
 * Triggers a purge immediately (admin endpoint / operational runbooks).
 *
 * @returns {string} Job ID.
 */
function triggerPurge() {
  return schedulePurge({ delayMs: 0 });
}

/**
 * Worker/queue/config snapshot for monitoring.
 *
 * @returns {object} `{ worker, queue, config }`.
 */
function getStats() {
  return {
    worker: purgeWorker.getStats(),
    queue: purgeQueue.getStats(),
    config: {
      retentionDays: getRetentionDays(),
      batchSize: getPurgeBatchSize(),
      maxBatches: getPurgeMaxBatches(),
      intervalMs: getIntervalMs(),
    },
  };
}

module.exports = {
  JOB_TYPE,
  runEscrowReadPurge,
  schedulePurge,
  startPurgeWorker,
  stopPurgeWorker,
  triggerPurge,
  getStats,
  getIntervalMs,
  purgeQueue,
  purgeWorker,
  // Exported for testing only — not part of the public API.
  _normaliseJob,
  _sanitiseOptions,
  MIN_INTERVAL_MS,
  MAX_INTERVAL_MS,
  MAX_DELAY_MS,
  MAX_BATCH_SIZE,
  MAX_MAX_BATCHES,
  DEFAULT_INTERVAL_MS,
};
