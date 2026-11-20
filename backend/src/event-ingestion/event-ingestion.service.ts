import { Injectable, OnModuleInit, OnModuleDestroy, Optional } from '@nestjs/common';
import { SanitizedLogger } from '../common/logging/sanitized-logger';
import { rpc as SorobanRpc, xdr } from '@stellar/stellar-sdk';
import { LedgerCursorService, LedgerCheckpoint } from './ledger-cursor.service';
import { EventProcessorService, SorobanEvent, ProcessedEvent } from './event-processor.service';
import { getStellarConfig } from '../stellar/stellar.config';
import { mapWithConcurrency } from '../common/concurrency';
import { config } from '../config/env.config';
import { buildSorobanServer } from '../stellar/soroban.helper';
import { IngestionHealthService } from '../monitoring/ingestion-health.service';

@Injectable()
export class EventIngestionService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new SanitizedLogger(EventIngestionService.name);
  private rpcServer!: SorobanRpc.Server;
  private pollingInterval: NodeJS.Timeout | null = null;
  private isRunning = false;
  private isIngestingInFlight = false;
  private readonly POLL_INTERVAL_MS = 5000;
  private readonly MAX_LEDGER_RANGE = 100;
  private readonly MAX_BACKOFF_MS = 5 * 60_000;
  private backoffUntil = 0;
  private consecutiveRpcFailures = 0;
  /** How many independent escrows to process in parallel per ingestion batch (#238). */
  private readonly eventProcessingConcurrency = config.EVENT_PROCESSING_CONCURRENCY;

  constructor(
    private readonly ledgerCursorService: LedgerCursorService,
    private readonly eventProcessorService: EventProcessorService,
    @Optional() private readonly ingestionHealth?: IngestionHealthService,
  ) {}

  onModuleInit() {
    this.rpcServer = buildSorobanServer(getStellarConfig().sorobanRpcUrl);
    this.logger.log('EventIngestionService initialized');
  }

  onModuleDestroy() {
    this.stopPolling();
  }

  async startPolling(contractId?: string): Promise<void> {
    if (this.isRunning) {
      this.logger.warn('Polling already running');
      return;
    }

    this.isRunning = true;
    this.logger.log('Starting event polling');

    const targetContract = contractId || getStellarConfig().contractId;

    this.pollingInterval = setInterval(() => void this.pollTick(targetContract), this.POLL_INTERVAL_MS);

    await this.ingestEvents(targetContract);
  }

  private async pollTick(targetContract: string): Promise<void> {
    try {
      // Skip this tick if a previous ingestion is still in-flight (#408)
      if (this.isIngestingInFlight) {
        this.logger.debug('Previous ingestion still in-flight, skipping this polling tick');
        return;
      }
      // Skip this tick while backing off from an RPC rate limit (#862)
      if (Date.now() < this.backoffUntil) return;
      await this.ingestEvents(targetContract);
      this.consecutiveRpcFailures = 0;
    } catch (error) {
      if (!this.applyRpcBackoff(error)) {
        this.logger.error('Error during polling:', error);
      }
    }
  }

  /**
   * On a 429 or 5xx from the RPC node, pause polling — honoring `Retry-After` when present,
   * otherwise doubling from the poll interval up to MAX_BACKOFF_MS (#862). Returns false for
   * any other error so it is logged as before.
   */
  private applyRpcBackoff(error: unknown): boolean {
    const response = (error as { response?: { status?: number; headers?: Record<string, unknown> } })
      ?.response;
    const status = response?.status;
    if (status === undefined || (status !== 429 && status < 500)) return false;

    this.consecutiveRpcFailures++;
    const retryAfterSeconds = Number(response?.headers?.['retry-after']);
    const delayMs =
      Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0
        ? retryAfterSeconds * 1000
        : Math.min(this.POLL_INTERVAL_MS * 2 ** this.consecutiveRpcFailures, this.MAX_BACKOFF_MS);
    this.backoffUntil = Date.now() + delayMs;
    this.logger.warn(`Soroban RPC returned ${status}; backing off polling for ${delayMs}ms`);
    return true;
  }

  stopPolling(): void {
    if (this.pollingInterval) {
      clearInterval(this.pollingInterval);
      this.pollingInterval = null;
    }
    this.isRunning = false;
    this.logger.log('Event polling stopped');
  }

  async ingestEvents(contractId: string): Promise<ProcessedEvent[]> {
    // Guard against overlapping ingestions (#408)
    if (this.isIngestingInFlight) {
      this.logger.debug('Ingestion already in-flight, skipping');
      return [];
    }

    this.isIngestingInFlight = true;
    try {
      const checkpoint = await this.ledgerCursorService.getCursor(contractId);
      const bounds = await this.getLedgerBounds();
      const currentLedger = bounds.latestLedger;
      const oldestLedger = bounds.oldestLedger;

      let startLedger: number;
      if (checkpoint) {
        // When the cursor records the last event id, re-read that ledger so later events in
        // the same ledger are not skipped; already-seen ids are filtered out below (#860).
        startLedger = checkpoint.lastEventId
          ? checkpoint.lastProcessedLedger
          : checkpoint.lastProcessedLedger + 1;
        if (startLedger < oldestLedger) {
          this.logger.warn(
            `Cursor ${startLedger} is behind oldest available ledger ${oldestLedger}. Fast-forwarding.`,
          );
          startLedger = oldestLedger;
        }
      } else {
        if (config.SOROBAN_START_LEDGER !== undefined) {
          startLedger = Math.max(config.SOROBAN_START_LEDGER, oldestLedger);
          this.logger.log(
            `No checkpoint found, starting from configured SOROBAN_START_LEDGER clamped to oldest: ${startLedger}`,
          );
        } else {
          startLedger = Math.max(currentLedger - 1000, oldestLedger);
          this.logger.log(
            `No checkpoint found and no config, starting from latest - 1000: ${startLedger}`,
          );
        }
      }

      const endLedger = Math.min(currentLedger, startLedger + this.MAX_LEDGER_RANGE - 1);

      if (startLedger > endLedger) {
        this.logger.debug('No new ledgers to process');
        return [];
      }

      this.logger.log(`Ingesting events from ledger ${startLedger} to ${endLedger}`);

      const lastEventId = checkpoint?.lastEventId;
      const lastLedger = checkpoint?.lastProcessedLedger;
      const events = (await this.fetchEvents(contractId, startLedger, endLedger))
        .sort((a, b) => a.ledger - b.ledger || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))
        .filter(e => !(lastEventId && e.ledger === lastLedger && e.id <= lastEventId));

      if (events.length === 0 && checkpoint && endLedger <= checkpoint.lastProcessedLedger) {
        return [];
      }

      const processedEvents = await this.processEventBatch(events);

      const lastEvent = events.length > 0 ? events[events.length - 1] : undefined;
      const latestProcessedLedger = lastEvent ? lastEvent.ledger : endLedger;
      const networkHash = await this.getNetworkHash();

      await this.ledgerCursorService.updateCursor(
        contractId,
        latestProcessedLedger,
        `${startLedger}-${endLedger}`,
        networkHash,
        lastEvent?.id,
      );

      return processedEvents;
    } finally {
      this.isIngestingInFlight = false;
    }
  }

  async ingestSingleLedger(contractId: string, ledger: number): Promise<ProcessedEvent[]> {
    const events = await this.fetchEvents(contractId, ledger, ledger);
    const processedEvents = await this.processEventBatch(events);

    const networkHash = await this.getNetworkHash();
    await this.ledgerCursorService.updateCursor(
      contractId,
      ledger,
      `single-${ledger}`,
      networkHash,
    );

    return processedEvents;
  }

  /**
   * Process a fetched batch of events with per-escrow ordering preserved and
   * independent escrows processed in parallel (#238).
   *
   * Concurrency-safety analysis:
   *  - `escrow_funded` / `escrow_released` / `escrow_disputed` carry the
   *    escrow id in `topic[1]`. Events with the *same* `topic[1]` MUST keep
   *    their relative order (e.g. funded before released), so they are grouped
   *    and each group runs sequentially.
   *  - `escrow_created` (and any unknown type) has no `topic[1]`. It cannot be
   *    correlated to a specific later keyed event, and a keyed event may
   *    depend on it (created must precede funded for the same escrow). So all
   *    id-less events run first, strictly in their original order, before any
   *    keyed group starts — no keyed handler can observe a missing escrow.
   *  - Groups for different escrow ids are independent (they touch different
   *    rows), so they run concurrently, bounded by `EVENT_PROCESSING_CONCURRENCY`.
   *  - `processEvent` swallows its own errors (returns `{ success: false }`),
   *    so per-event isolation is unchanged.
   *
   * The returned array is ordered id-less-first then by group; callers use it
   * for counts/status only (the ledger cursor is advanced from `events`, not
   * from this array).
   */
  async processEventBatch(events: SorobanEvent[]): Promise<ProcessedEvent[]> {
    // Sort by (ledger, event id) to ensure processing order never depends on RPC page order (#408)
    const sortedEvents = [...events].sort((a, b) => {
      if (a.ledger !== b.ledger) return a.ledger - b.ledger;
      return a.id.localeCompare(b.id);
    });

    const unkeyed: SorobanEvent[] = [];
    const keyed = new Map<string, SorobanEvent[]>();

    for (const event of sortedEvents) {
      const key = event.topic[1];
      if (key === undefined || key === '') {
        unkeyed.push(event);
        continue;
      }
      let bucket = keyed.get(key);
      if (!bucket) {
        bucket = [];
        keyed.set(key, bucket);
      }
      bucket.push(event);
    }

    const processed: ProcessedEvent[] = [];

    // Phase 1 — id-less events (escrow_created, unknown), strict original order.
    for (const event of unkeyed) {
      processed.push(await this.eventProcessorService.processEvent(event));
    }

    // Phase 2 — one group per escrow id, groups in parallel, sequential within.
    const groupResults = await mapWithConcurrency(
      [...keyed.values()],
      this.eventProcessingConcurrency,
      async group => {
        const groupOut: ProcessedEvent[] = [];
        for (const event of group) {
          groupOut.push(await this.eventProcessorService.processEvent(event));
        }
        return groupOut;
      },
    );
    for (const result of groupResults) {
      if (result.status === 'fulfilled') {
        processed.push(...result.value);
      }
    }

    return processed;
  }

  private async fetchEvents(
    contractId: string,
    startLedger: number,
    endLedger: number,
  ): Promise<SorobanEvent[]> {
    try {
      const allEvents: SorobanEvent[] = [];
      let currentStart = startLedger;
      let cursor: string | undefined;

      while (currentStart <= endLedger) {
        const batchEnd = Math.min(currentStart + 99, endLedger);

        // Build request parameters — when using cursor, omit startLedger and endLedger
        const getEventsParams: any = {
          filters: [
            {
              type: 'contract',
              contractIds: [contractId],
            },
          ],
          limit: 100,
        };

        if (cursor) {
          // Use cursor-based pagination for the current batch
          getEventsParams.cursor = cursor;
          // When using cursor, endLedger can still be specified to bound the result
          getEventsParams.endLedger = batchEnd + 1; // +1 because endLedger is exclusive
        } else {
          // Start new batch with explicit ledger bounds
          getEventsParams.startLedger = currentStart;
          getEventsParams.endLedger = batchEnd + 1; // +1 because endLedger is exclusive
        }

        const response = (await this.rpcServer.getEvents(getEventsParams)) as any;
        allEvents.push(...response.events.map((event: any) => this.parseEvent(event)));

        // Check if more events exist in this batch via cursor
        if (response.cursor && response.events.length === 100) {
          // More events exist in this ledger range, continue with cursor
          cursor = response.cursor;
        } else {
          // No more events in this batch, move to next batch
          cursor = undefined;
          currentStart = batchEnd + 1;
        }
      }

      // Drop events re-delivered by an overlapping page, e.g. after an RPC failover mid-poll (#861)
      const seen = new Set<string>();
      return allEvents.filter(e => {
        if (e.ledger < startLedger || e.ledger > endLedger || seen.has(e.id)) return false;
        seen.add(e.id);
        return true;
      });
    } catch (error) {
      this.logger.error(`Failed to fetch events for ledgers ${startLedger}-${endLedger}:`, error);
      throw error;
    }
  }

  private parseEvent(event: SorobanRpc.Api.EventResponse): SorobanEvent {
    const parsedValue = this.parseEventValue(event.value);
    const topics = event.topic.map(t => this.parseTopic(t));

    return {
      id: event.id,
      ledger: event.ledger,
      contractId: event.contractId?.toString() || '',
      eventType: topics[0] || 'unknown',
      topic: topics,
      value: typeof parsedValue === 'string' ? { raw: parsedValue } : parsedValue,
      xdr: event.value.toXDR().toString('base64'),
      createdAt: new Date(),
    };
  }

  private parseEventValue(value: xdr.ScVal): Record<string, unknown> | string {
    try {
      if ((value.switch().name as string) === 'scvBytes') {
        const bytes = value.bytes();
        return JSON.parse(Buffer.from(bytes).toString()) as Record<string, unknown>;
      }
      return value.toXDR().toString();
    } catch {
      return value.toXDR().toString();
    }
  }

  private parseTopic(topic: xdr.ScVal): string {
    try {
      if ((topic.switch().name as string) === 'scvSymbol') {
        return topic.sym().toString();
      }
      if ((topic.switch().name as string) === 'scvBytes') {
        return Buffer.from(topic.bytes()).toString();
      }
      return topic.toXDR().toString();
    } catch {
      return 'unknown';
    }
  }

  private async getLedgerBounds(): Promise<{ latestLedger: number; oldestLedger: number }> {
    try {
      const response = await this.rpcServer.getHealth();
      // Reaching here means the RPC node actually answered — the signal `/health`'s
      // event-ingestion check needs, since a poller stuck retrying a bad endpoint never
      // gets this far but keeps its process/loop alive (#987).
      this.ingestionHealth?.recordSuccessfulPoll(this.POLL_INTERVAL_MS);
      if (response && typeof response === 'object') {
        const anyResp = response as any;
        return {
          latestLedger: anyResp.latestLedger ?? anyResp.latest_ledger ?? 0,
          oldestLedger: anyResp.oldestLedger ?? anyResp.oldest_ledger ?? 0,
        };
      }
      return { latestLedger: 0, oldestLedger: 0 };
    } catch (error) {
      this.logger.error('Failed to get ledger bounds:', error);
      return { latestLedger: 0, oldestLedger: 0 };
    }
  }

  private async getNetworkHash(): Promise<string> {
    try {
      const network = await this.rpcServer.getNetwork();
      return network.passphrase;
    } catch (error) {
      this.logger.error('Failed to get network hash:', error);
      return '';
    }
  }

  async handleReorg(contractId: string, fromLedger: number): Promise<void> {
    this.logger.warn(`Handling reorg from ledger ${fromLedger}`);

    // Clear events from fromLedger onward (not before), so reprocessing includes the reorg point (#408)
    await this.eventProcessorService.clearEventsFromLedger(fromLedger);
    await this.ledgerCursorService.updateCursor(
      contractId,
      fromLedger - 1,
      `reorg-${fromLedger}`,
      '',
    );

    this.logger.log(`Reorg handled, reprocessing from ledger ${fromLedger}`);
  }

  async getStatus(): Promise<{
    isRunning: boolean;
    checkpoint?: LedgerCheckpoint;
    failedEvents: number;
  }> {
    const checkpoint = await this.ledgerCursorService.getCursor(getStellarConfig().contractId);
    const failedEvents = await this.eventProcessorService.getFailedEvents();

    return {
      isRunning: this.isRunning,
      checkpoint,
      failedEvents: failedEvents.length,
    };
  }

  async retryFailedEvents(): Promise<ProcessedEvent[]> {
    // Delegate to EventProcessorService.retryFailedEvents() which correctly clears
    // the stale failed record before re-invoking processEvent (#778).
    return this.eventProcessorService.retryFailedEvents();
  }
}
