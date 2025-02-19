import { v } from "convex/values";
import {
  internalAction,
  internalMutation,
  internalQuery,
  MutationCtx,
} from "./_generated/server";
import { FunctionHandle, WithoutSystemFields } from "convex/server";
import { Doc, Id, TableNames } from "./_generated/dataModel";
import { createLogger, getDefaultLogLevel, Logger } from "./logger";
import { internal } from "./_generated/api";
import { FrozenOptions, runResult, RunResult } from "./schema";
import { trigger } from "./mainLoop";
import { updateErrorStats } from "./stats";

const WHEEL_SEGMENT_MS = 125;

function toWheelSegment(ms: number) {
  return Math.floor(ms / WHEEL_SEGMENT_MS);
}

export function fromWheelSegment(segment: number) {
  return segment * WHEEL_SEGMENT_MS;
}

const QUEUE_WINDOW = 100;

async function pullFromQueue<T extends TableNames, D extends Doc<T>>(
  ctx: MutationCtx,
  tableName: T,
  window?: number
): Promise<[D[], boolean]> {
  const takeWindow = window ?? QUEUE_WINDOW;
  const items = await ctx.db.query(tableName).take(takeWindow + 1);
  const is_more = items.length > takeWindow;
  return [items.slice(0, takeWindow), is_more];
}

// Phase 1. Incoming
async function processIncoming(
  logger: Logger,
  ctx: MutationCtx
): Promise<boolean> {
  const [incoming, is_more] = await pullFromQueue(ctx, "incoming");

  for (const job of incoming) {
    await addToCommitted(logger, ctx, job);
  }
  return is_more;
}

async function addToCommitted(
  logger: Logger,
  ctx: MutationCtx,
  job: Doc<"incoming">
) {
  const desiredRunTime = job._creationTime + job.options.initialDelayMs;

  // Commit to run.
  const committed = await ctx.db.insert("committed", {
    handle: job.handle,
    arguments: job.arguments,
    retries: 0,
    incomingId: job._id,
    enqueuedAt: job._creationTime,
    canceled: false,
    options: job.options,
  });
  logger.debug(
    `Incoming job ${job._id} processed (now mapped to ${committed})`
  );
  await ctx.db.insert("wheel", {
    job: committed,
    segment: toWheelSegment(desiredRunTime),
  });

  // Remove from incoming.
  await ctx.db.delete(job._id);
}

const defaultFrozenOptions: FrozenOptions = {
  logLevel: getDefaultLogLevel(),
  maxParallelism: 0, //
};

async function getConfig(ctx: MutationCtx): Promise<[FrozenOptions, Logger]> {
  let frozen = await ctx.db.query("frozenConfig").first();
  const lastCommitted = await ctx.db.query("committed").order("desc").first();
  if (lastCommitted) {
    if (!frozen) {
      const frozenId = await ctx.db.insert("frozenConfig", {
        config: {
          logLevel: lastCommitted.options.logLevel,
          maxParallelism: lastCommitted.options.maxParallelism,
        },
        frozenAt: lastCommitted._creationTime,
      });
      frozen = await ctx.db.get(frozenId);
    } else if (frozen.frozenAt < lastCommitted?._creationTime) {
      frozen.config = {
        logLevel: lastCommitted.options.logLevel,
        maxParallelism: lastCommitted.options.maxParallelism,
      };
      frozen.frozenAt = lastCommitted._creationTime;
      await ctx.db.replace(frozen._id, frozen);
    }
  }
  const config = frozen?.config ?? defaultFrozenOptions;
  const logger = createLogger(config.logLevel);
  return [config, logger];
}

// Phase 2. Cancellations.
async function processCancellations(
  logger: Logger,
  ctx: MutationCtx,
  toFinalize: WithoutSystemFields<Doc<"ended">>[]
): Promise<boolean> {
  const [cancellations, is_more] = await pullFromQueue(ctx, "cancellations");

  for (const cancellation of cancellations) {
    logger.debug(`Processing cancellation request for ${cancellation.job}`);
    await ctx.db.delete(cancellation._id);

    // First, check the incoming queue
    const incoming = await ctx.db.get(cancellation.job);
    if (incoming) {
      // Consolidate logic on cancelling only committed jobs.
      await addToCommitted(logger, ctx, incoming);
    }

    const committed = await ctx.db
      .query("committed")
      .withIndex("by_incoming", (q) =>
        q.eq("canceled", false).eq("incomingId", cancellation.job)
      )
      .first();
    if (!committed) {
      console.warn(
        "No incoming or committed job found for cancellation",
        cancellation
      );
      continue;
    }
    await ctx.db.patch(committed._id, {
      canceled: true,
    });
    // Is it in the wheel still?
    const wheelJob = await ctx.db
      .query("wheel")
      .withIndex("by_job", (q) => q.eq("job", committed._id))
      .first();
    if (wheelJob) {
      // Prevent run -- remove from wheel.
      await ctx.db.delete(wheelJob._id);
      toFinalize.push({
        job: committed._id,
        result: { type: "canceled" },
        endedAt: Date.now(),
      });
    } else {
      // Is it still running?
      const running = await ctx.db
        .query("running")
        .withIndex("by_job", (q) => q.eq("job", committed._id))
        .first();
      if (running) {
        await cancelIfRunning(logger, ctx, running.schedulerId);
      }
    }
  }
  return is_more;
}

// Phase 3. Account for gracefully ended jobs.
async function processEnded(
  logger: Logger,
  ctx: MutationCtx,
  toFinalize: WithoutSystemFields<Doc<"ended">>[]
): Promise<boolean> {
  const [ended, is_more] = await pullFromQueue(ctx, "ended");

  for (const e of ended) {
    logger.debug(
      `Processing cleanly ended job ${e.job} with result ${e.result}`
    );
    // Remove from ended.
    await ctx.db.delete(e._id);
    toFinalize.push({
      job: e.job,
      result: e.result,
      endedAt: e.endedAt,
    });
  }
  return is_more;
}

const RUNNING_POLL_INTERVAL = 60 * 1000; // 1 minute.

// Phase 4. Poll running jobs for things the scheduler sees as finished.
async function processRunning(
  logger: Logger,
  ctx: MutationCtx,
  loopState: Doc<"loopState">,
  config: FrozenOptions,
  toFinalize: WithoutSystemFields<Doc<"ended">>[]
): Promise<number> {
  const running = await ctx.db.query("running").collect();
  let countRunning = running.length;

  // Poll statuses if we have pending finalizations or it's time to check on
  // unusual error states (timeouts, etc).
  if (
    toFinalize.length > 0 ||
    (loopState.lastLivePoll ?? 0) + RUNNING_POLL_INTERVAL < Date.now()
  ) {
    loopState.lastLivePoll = Date.now();
    for (const r of running) {
      const status = await ctx.db.system.get(r.schedulerId);
      logger.debug(
        `Polling running job ${r.job} (status=${status?.state.kind})`
      );
      if (!status) {
        logger.warn("Unusual -- running job not found in scheduler");
        countRunning--;
        toFinalize.push({
          job: r.job,
          result: {
            type: "failed",
            error: "Scheduler system error",
          },
          endedAt: Date.now(),
        });
        await ctx.db.delete(r._id);
      } else if (status.state.kind === "inProgress") {
        // NOOP -- still running.
      } else if (status.state.kind === "pending") {
        // NOOP -- should run ASAP.
      } else if (status.state.kind === "success") {
        // Finalization is handled by the ended queue.
        countRunning--;
        await ctx.db.delete(r._id);
      } else if (status.state.kind === "canceled") {
        logger.debug(`Discovered that Convex scheduler canceled job ${r.job}`);
        countRunning--;
        toFinalize.push({
          job: r.job,
          result: { type: "canceled" },
          endedAt: Date.now(),
        });
        await ctx.db.delete(r._id);
      } else if (status.state.kind === "failed") {
        console.log(
          "Unusual -- running job failed without error propagation",
          r
        );
        countRunning--;
        toFinalize.push({
          job: r.job,
          result: { type: "failed", error: "Scheduler system error" },
          endedAt: Date.now(),
        });
        await ctx.db.delete(r._id);
      }
    }
  }
  return config.maxParallelism
    ? Math.max(0, config.maxParallelism - countRunning)
    : 0;
}

// Phase 5. Finalization of jobs we're removing.
export async function handleFinalizations(
  logger: Logger,
  ctx: MutationCtx,
  toFinalize: WithoutSystemFields<Doc<"ended">>[]
) {
  for (const f of toFinalize) {
    const committed = await ctx.db.get(f.job);
    if (!committed) {
      throw new Error("Committed job not found for finalization");
    }

    if (f.result.type === "success") {
      logger.debug(
        `Finalizing successful job ${committed._id} with result ${f.result}`
      );
      await runOnComplete(logger, ctx, committed, f.result);
      await ctx.db.delete(committed._id);
      await updateErrorStats(
        ctx,
        committed._id,
        committed.retries,
        false,
        f.endedAt - committed.enqueuedAt
      );
      logger.event("job-end", {
        jobId: committed.incomingId,
        annotation: committed.options.annotation ?? undefined,
        result: "success",
        retries: committed.retries,
        durationMs: f.endedAt - committed.enqueuedAt,
      });
    } else {
      if (committed.retries >= committed.options.maxRetries) {
        logger.debug(
          `Out of retries -- finalizing permanently failed job ${committed._id} with result ${f.result}`
        );
        await runOnComplete(logger, ctx, committed, f.result);
        await ctx.db.delete(committed._id);
        await updateErrorStats(
          ctx,
          committed._id,
          committed.retries,
          true,
          f.endedAt - committed.enqueuedAt
        );
        logger.event("job-end", {
          jobId: committed.incomingId,
          annotation: committed.options.annotation ?? undefined,
          result: "failed",
          retries: committed.retries,
          durationMs: f.endedAt - committed.enqueuedAt,
        });
      } else if (committed.canceled) {
        logger.debug(`Finalizing canceled job ${committed._id}`);
        await runOnComplete(logger, ctx, committed, { type: "canceled" });
        await ctx.db.delete(committed._id);
        await updateErrorStats(
          ctx,
          committed._id,
          committed.retries,
          false,
          f.endedAt - committed.enqueuedAt
        );
        logger.event("job-end", {
          jobId: committed.incomingId,
          annotation: committed.options.annotation ?? undefined,
          result: "canceled",
          durationMs: f.endedAt - committed.enqueuedAt,
        });
      } else {
        // Need to reschedule!
        committed.retries++;
        logger.debug(
          `Error during execution: rescheduling job ${committed._id} (retry #${committed.retries})`
        );
        await ctx.db.patch(committed._id, committed);
        const retryInMs = await rescheduleJob(logger, ctx, committed);
        await updateErrorStats(ctx, committed._id, committed.retries, true);
        logger.event("job-retry", {
          jobId: committed.incomingId,
          annotation: committed.options.annotation ?? undefined,
          retries: committed.retries,
          durationMs: f.endedAt - committed.enqueuedAt,
          retryInMs,
        });
      }
    }
  }
}

async function runOnComplete(
  logger: Logger,
  ctx: MutationCtx,
  run: Doc<"committed">,
  result: RunResult
) {
  if (!run.options.onComplete) {
    return;
  }
  try {
    logger.debug(`Running onComplete handler for ${run._id}`);
    const handle = run.options.onComplete as FunctionHandle<
      "mutation",
      { runId: Id<"incoming">; context: unknown; result: RunResult }
    >;
    await ctx.runMutation(handle, {
      runId: run.incomingId,
      context: run.options.context,
      result,
    });
    logger.debug(`Finished running onComplete handler for ${run._id}`);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } catch (e: any) {
    logger.error(
      `Error running onComplete handler for ${run._id}: ${e.message}`
    );
  }
}

async function rescheduleJob(
  logger: Logger,
  ctx: MutationCtx,
  committed: Doc<"committed">
): Promise<number> {
  const backoffMs =
    committed.options.initialBackoffMs *
    Math.pow(committed.options.base, committed.retries - 1);
  const nextAttempt = withJitter(backoffMs);
  const startTime = Date.now() + nextAttempt;
  const segment = toWheelSegment(startTime);
  await ctx.db.insert("wheel", {
    job: committed._id,
    segment,
  });
  return nextAttempt;
}

export function withJitter(delay: number) {
  return delay * (0.5 + Math.random());
}

// Phase 6. Run jobs from the wheel.
async function runWheelJobs(logger: Logger, ctx: MutationCtx, count: number) {
  logger.debug(`Running any ready wheel jobs (available threads = ${count})`);
  const maxSegment = toWheelSegment(Date.now());
  const wheelJobs = await ctx.db
    .query("wheel")
    .withIndex("by_segment")
    .take(count);
  let running = 0;
  for (const job of wheelJobs) {
    if (running === count) {
      logger.debug(`Reached max wheel jobs to run (count=${count})`);
      break;
    }
    if (job.segment > maxSegment) {
      logger.debug(`No more wheel jobs to run (maxSegment=${maxSegment})`);
      break;
    }
    await ctx.db.delete(job._id);
    const committed = await ctx.db.get(job.job);
    if (!committed) {
      throw new Error("Committed job not found for wheel job");
    }
    const schedulerId = await ctx.scheduler.runAfter(0, internal.lib.execute, {
      runId: committed._id,
    });
    logger.debug(
      `... running wheel job ${job.job} (schedulerId=${schedulerId})`
    );
    await ctx.db.insert("running", {
      job: job.job,
      schedulerId,
    });
    running++;
  }
  logger.debug(`Spawned ${running} more jobs`);
  // Did we fill the job pool?
  return running === count;
}

export const execute = internalAction({
  args: {
    runId: v.id("committed"),
  },
  handler: async (ctx, args) => {
    const run = await ctx.runQuery(internal.lib.load, { runId: args.runId });
    if (!run) {
      throw new Error("Run not found");
    }
    const logger = createLogger(run.options.logLevel);
    logger.debug(`Executing run ${args.runId}`, run);

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const handle = run.handle as FunctionHandle<"action", any, any>;
    let result: RunResult;
    let endedAt;
    try {
      const startTime = Date.now();
      logger.debug(`Starting executing ${args.runId}`);
      const functionResult = await ctx.runAction(handle, run.arguments);
      endedAt = Date.now();
      const duration = endedAt - startTime;
      logger.debug(
        `Finished executing ${args.runId} (${duration.toFixed(2)}ms)`
      );
      result = {
        type: "success",
        returnValue: functionResult,
      };
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (e: any) {
      endedAt = Date.now();
      logger.error(`Error executing ${args.runId}: ${e.message}`);
      result = {
        type: "failed",
        error: e.message,
      };
    }
    await ctx.runMutation(internal.lib.saveResult, {
      runId: args.runId,
      result,
      endedAt,
    });
  },
});

export const load = internalQuery({
  args: {
    runId: v.id("committed"),
  },
  handler: async (ctx, args) => {
    return await ctx.db.get(args.runId);
  },
});

export const saveResult = internalMutation({
  args: {
    runId: v.id("committed"),
    result: runResult,
    endedAt: v.number(),
  },
  handler: async (ctx, args) => {
    const logger = createLogger("DEBUG");
    await enqueueEnded(logger, ctx, {
      job: args.runId,
      result: args.result,
      endedAt: args.endedAt,
    });
  },
});

export const loop = internalMutation({
  args: {},
  handler: async (ctx) => {
    const [config, logger] = await getConfig(ctx);
    logger.debug("Starting loop");

    // Cancel a delayed call if one is registered.
    let loopState = await ctx.db.query("loopState").first();
    if (!loopState) {
      logger.debug("No loop state found -- creating");
      const loopId = await ctx.db.insert("loopState", {});
      loopState = await ctx.db.get(loopId);
      if (!loopState) {
        throw new Error("Loop state not found");
      }
    } else {
      const wake = loopState.wake;
      if (wake) {
        logger.debug("Loop wake found -- cancelling delayed call");
        await cancelIfRunning(logger, ctx, wake);
        await ctx.db.patch(loopState._id, { wake: undefined });
      }
    }
    await advance(logger, ctx, loopState, config);
  },
});

export const reloop = internalMutation({
  args: {},
  handler: async (ctx) => {
    const [config, logger] = await getConfig(ctx);
    const loopState = await ctx.db.query("loopState").first();
    if (!loopState) {
      throw new Error("Loop state not found");
    }
    await ctx.db.patch(loopState._id, { wake: undefined });
    await advance(logger, ctx, loopState, config);
  },
});

async function advance(
  logger: Logger,
  ctx: MutationCtx,
  loopState: Doc<"loopState">,
  config: FrozenOptions
) {
  // Handle incoming jobs.
  const moreIncoming = await processIncoming(logger, ctx);
  // A new job might have a changed config.
  [config, logger] = await getConfig(ctx);

  // Handle cancellations.
  const toFinalize: WithoutSystemFields<Doc<"ended">>[] = [];
  const moreCancellations = await processCancellations(logger, ctx, toFinalize);

  // Handle explicitly ended jobs.
  const moreEnded = await processEnded(logger, ctx, toFinalize);

  // Reap any running jobs that have finished and discover how much capacity
  // we have in the pool for new jobs.
  const countRunnable = await processRunning(
    logger,
    ctx,
    loopState,
    config,
    toFinalize
  );

  // Finalize or retry any jobs that need it.
  await handleFinalizations(logger, ctx, toFinalize);

  // Run next eligible wheel jobs, if any (starting with oldest)
  const poolFull = await runWheelJobs(logger, ctx, countRunnable);

  // Finally, schedule the next loop.
  const wake = await scheduleNextLoop(
    logger,
    ctx,
    moreIncoming || moreCancellations || moreEnded,
    poolFull
  );
  await ctx.db.patch(loopState._id, {
    wake: wake ?? undefined,
    lastLivePoll: loopState.lastLivePoll,
  });
}

// Even if the wheel doesn't have any jobs anytime soon,
// we need to poll the running jobs every minute to discover
// hung jobs or timed out jobs, etc, from the Convex scheduler.
//
// So we'll never sleep longer than this if there are running jobs.
const MAX_JOB_POLL = 60 * 1000; // 1 minute.

async function scheduleNextLoop(
  logger: Logger,
  ctx: MutationCtx,
  unprocessedQueues: boolean,
  poolFull: boolean
): Promise<Id<"_scheduled_functions"> | null> {
  // If we didn't process some entries in our queues, we need to wake up
  // immediately go keep the system moving.
  if (unprocessedQueues) {
    logger.debug("Unprocessed queues -- waking up immediately");
    await trigger(ctx);
    return null;
  } else {
    // Get the minimum wheel segment.
    const SENTINEL = Number.MAX_SAFE_INTEGER;
    let wheelTime = SENTINEL;
    if (!poolFull) {
      const wheelSegment = await ctx.db
        .query("wheel")
        .withIndex("by_segment")
        .first();
      // More jobs need to be scheduled in the future?
      if (wheelSegment) {
        wheelTime = fromWheelSegment(wheelSegment.segment);
      }
    }
    // Jobs are running so we need to poll them.
    let pollTime = SENTINEL;
    if ((await ctx.db.query("running").first()) !== null) {
      pollTime = Date.now() + MAX_JOB_POLL;
    }

    // Either sleep until the next wheel job is ready or until we need to poll
    // the running jobs.
    const wakeTime =
      wheelTime === SENTINEL && pollTime === SENTINEL
        ? null
        : Math.min(wheelTime, pollTime);
    if (wakeTime !== null) {
      const delta = Math.max(0, wakeTime - Date.now());
      logger.debug(
        `Scheduling next loop in ${delta}ms (wt=${wheelTime}, pt=${pollTime})`
      );
      const wake = await ctx.scheduler.runAfter(delta, internal.lib.reloop);
      return wake;
    }
    logger.debug("No more work to do -- going idle");
    return null;
  }
}

export async function enqueueIncoming(
  logger: Logger,
  ctx: MutationCtx,
  job: WithoutSystemFields<Doc<"incoming">>
): Promise<Id<"incoming">> {
  const id = await ctx.db.insert("incoming", job);
  await trigger(ctx);
  return id;
}

export async function enqueueCancellation(
  logger: Logger,
  ctx: MutationCtx,
  job: WithoutSystemFields<Doc<"cancellations">>
) {
  await ctx.db.insert("cancellations", job);
  await trigger(ctx);
}

export async function enqueueEnded(
  logger: Logger,
  ctx: MutationCtx,
  job: WithoutSystemFields<Doc<"ended">>
) {
  await ctx.db.insert("ended", job);
  await trigger(ctx);
}

async function cancelIfRunning(
  logger: Logger,
  ctx: MutationCtx,
  job: Id<"_scheduled_functions">
): Promise<boolean> {
  const status = await ctx.db.system.get(job);
  logger.debug(
    `Cancelling scheduler job ${job} (status=${status?.state.kind})`
  );
  if (status?.state.kind === "pending") {
    logger.debug(`Cancelling scheduler job ${job}`);
    await ctx.scheduler.cancel(job);
    return true;
  } else {
    logger.debug(`Scheduler job ${job} is not running`);
  }
  return false;
}
