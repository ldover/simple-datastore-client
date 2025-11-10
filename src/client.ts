type CleanupFn = () => void;

type EventPayload = Record<string, unknown>;

export interface PushEvent<TPayload extends EventPayload = EventPayload> {
  id: string;
  payload: TPayload;
}

export interface SyncOptions<TPayload extends EventPayload = EventPayload> {
  since?: number | null;
  events?: Array<PushEvent<TPayload>>;
}

export interface PullOptions {
  since?: number | null;
}

export interface Acknowledgement {
  id: string | null;
  success: boolean;
  error: string | null;
}

export interface SyncResult<TEntity = Record<string, unknown>> {
  result: TEntity[];
  acknowledgements: Acknowledgement[];
}

type WebSocketEventHandler = (...args: any[]) => void; // eslint-disable-line @typescript-eslint/no-explicit-any

interface WebSocketLike {
  send(data: string): void;
  close(): void;
  addEventListener?(event: string, handler: WebSocketEventHandler): void;
  removeEventListener?(event: string, handler: WebSocketEventHandler): void;
  on?(event: string, handler: WebSocketEventHandler): void;
  off?(event: string, handler: WebSocketEventHandler): void;
  removeListener?(event: string, handler: WebSocketEventHandler): void;
}

type WebSocketFactory = new (url: string) => WebSocketLike;

type FramePayload = { frame?: unknown; error?: Error };
type FrameWaiter = (payload: FramePayload) => void;

const frameQueue = new WeakMap<WebSocketLike, FramePayload[]>();
const frameWaiters = new WeakMap<WebSocketLike, FrameWaiter[]>();

const DEBUG_ENABLED =
  typeof process !== 'undefined' &&
  typeof process.env !== 'undefined' &&
  process.env.DATASTORE_DEBUG === '1';

function debugLog(...args: unknown[]) {
  if (!DEBUG_ENABLED) return;
  // eslint-disable-next-line no-console
  console.debug('[SimpleDatastoreClient]', ...args);
}

export interface SimpleDatastoreClientOptions {
  baseUrl: string;
  token: string;
  WebSocketClass?: WebSocketFactory;
  defaultTimeoutMs?: number;
}

const textDecoder =
  typeof TextDecoder !== 'undefined' ? new TextDecoder('utf-8') : null;

export class SimpleDatastoreClient {
  private readonly baseUrl: string;

  private readonly token: string;

  private readonly timeoutMs: number;

  private readonly WebSocketClass: WebSocketFactory;

  constructor(options: SimpleDatastoreClientOptions) {
    if (!options || typeof options !== 'object') {
      throw new Error('SimpleDatastoreClient requires an options object');
    }

    const {
      baseUrl,
      token,
      WebSocketClass,
      defaultTimeoutMs = 10000,
    } = options;

    if (!baseUrl) {
      throw new Error('SimpleDatastoreClient requires a baseUrl');
    }
    if (!token) {
      throw new Error('SimpleDatastoreClient requires an auth token');
    }

    this.baseUrl = baseUrl;
    this.token = token;
    this.timeoutMs = defaultTimeoutMs;
    const globalWs = (globalThis as { WebSocket?: WebSocketFactory }).WebSocket;
    const resolvedWs = WebSocketClass || globalWs;

    if (!resolvedWs) {
      throw new Error(
        'No WebSocket implementation provided. Pass WebSocketClass explicitly when running in Node.'
      );
    }
    this.WebSocketClass = resolvedWs;
    debugLog('Initialized client', { baseUrl: this.baseUrl });
  }

  async pull<TEntity = Record<string, unknown>>(
    options: PullOptions = {}
  ): Promise<TEntity[]> {
    const result = await this.sync<TEntity>({ since: options.since ?? null });
    return result.result;
  }

  async push(events: PushEvent[]): Promise<Acknowledgement[]> {
    const result = await this.sync({ events });
    return result.acknowledgements;
  }

  async sync<TEntity = Record<string, unknown>>(
    options: SyncOptions = {}
  ): Promise<SyncResult<TEntity>> {
    const { since = null, events = [] } = options;
    debugLog('sync:start', { since, eventCount: events.length });
    const socket = await this.#openConnection({ since });
    this.#prepareSocket(socket);
    let pulled: TEntity[] = [];
    const acknowledgements: Acknowledgement[] = [];

    try {
      if (since !== null && since !== undefined) {
        const frame = await this.#waitForFrame(socket);
        if (!Array.isArray(frame) || frame[0] !== 'RESULT') {
          throw new Error(
            `Unexpected frame while waiting for pull RESULT: ${JSON.stringify(
              frame
            )}`
          );
        }
        pulled = Array.isArray(frame[1]) ? (frame[1] as TEntity[]) : [];
      }

      for (const event of events) {
        const { id, payload } = event || {};
        if (typeof id !== 'string' || !id) {
          throw new Error('Each event must include a non-empty string id');
        }
        if (!payload || typeof payload !== 'object') {
          throw new Error('Each event must include a payload object');
        }

        const wireFrame = ['EVENT', id, payload] as const;
        socket.send(JSON.stringify(wireFrame));
        const ack = await this.#waitForFrame(socket);
        if (!Array.isArray(ack) || ack[0] !== 'OK') {
          throw new Error(
            `Unexpected frame while waiting for OK: ${JSON.stringify(ack)}`
          );
        }
        acknowledgements.push({
          id: (ack[1] as string) ?? null,
          success: Boolean(ack[2]),
          error: (ack[3] as string) ?? null,
        });
      }
    } finally {
      socket.close();
    }

    debugLog('sync:complete', {
      pulled: pulled.length,
      acknowledgements: acknowledgements.length
    });
    return { result: pulled, acknowledgements };
  }

  #buildUrl({ since }: { since: number | null }) {
    const url = new URL(this.baseUrl);
    url.searchParams.set('token', this.token);
    if (since !== null && since !== undefined) {
      url.searchParams.set('since', String(since));
    }
    return url.toString();
  }

  #openConnection({ since }: { since: number | null }) {
    const url = this.#buildUrl({ since });
    return new Promise<WebSocketLike>((resolve, reject) => {
      const socket = new this.WebSocketClass(url);
      this.#prepareSocket(socket);
      const cleanup: CleanupFn[] = [];

      const onError = (err: unknown) => {
        cleanup.forEach((fn) => fn());
        debugLog('socket:error', err);
        reject(err instanceof Error ? err : new Error(String(err)));
      };
      const onOpen = () => {
        cleanup.forEach((fn) => fn());
        debugLog('socket:open', { url });
        resolve(socket);
      };

      cleanup.push(this.#attach(socket, 'error', onError));
      cleanup.push(this.#attach(socket, 'open', onOpen));
    });
  }

  #waitForFrame(socket: WebSocketLike) {
    this.#prepareSocket(socket);
    const queue = frameQueue.get(socket);
    if (queue && queue.length > 0) {
      const payload = queue.shift()!;
      if (payload.error) {
        debugLog('frame:queue-reject', payload.error);
        return Promise.reject(payload.error);
      }
      debugLog('frame:queue-resolve', payload.frame);
      return Promise.resolve(payload.frame);
    }

    return new Promise<unknown>((resolve, reject) => {
      const removeListeners: CleanupFn[] = [];

      removeListeners.push(
        this.#attach(socket, 'close', (event?: { code?: number; reason?: string }) => {
          debugLog('socket:close', { code: event?.code, reason: event?.reason });
          cleanup();
          reject(new Error('Connection closed before response was received'));
        })
      );

      const timer = setTimeout(() => {
        cleanup();
        reject(new Error('Timed out waiting for datastore response'));
      }, this.timeoutMs);

      const cleanup = () => {
        removeWaiter();
        clearTimeout(timer);
        removeListeners.forEach((fn) => fn());
      };

      const finish: FrameWaiter = (payload) => {
        cleanup();
        if (payload.error) {
          debugLog('frame:reject', payload.error);
          reject(payload.error);
        } else {
          debugLog('frame:resolve', payload.frame);
          resolve(payload.frame);
        }
      };

      const removeWaiter = () => {
        const waiters = frameWaiters.get(socket);
        if (!waiters) return;
        const index = waiters.indexOf(finish);
        if (index >= 0) {
          waiters.splice(index, 1);
        }
      };

      const waiters = frameWaiters.get(socket);
      if (!waiters) {
        throw new Error('Socket not initialized for datastore client');
      }
      waiters.push(finish);

      removeListeners.push(
        this.#attach(socket, 'error', (err: unknown) => {
          cleanup();
          reject(err instanceof Error ? err : new Error(String(err)));
        })
      );
      removeListeners.push(
        this.#attach(socket, 'close', () => {
          cleanup();
          reject(new Error('Connection closed before response was received'));
        })
      );
    });
  }

  #prepareSocket(socket: WebSocketLike) {
    if (frameQueue.has(socket)) {
      return;
    }
    frameQueue.set(socket, []);
    frameWaiters.set(socket, []);

    this.#attach(socket, 'message', (event: unknown) => {
      try {
        const raw = extractData(event);
        const text = normalizePayload(raw);
        const parsed = JSON.parse(text);
        debugLog('frame:incoming', parsed);
        this.#deliverFrame(socket, { frame: parsed });
      } catch (err) {
        debugLog('frame:incoming-error', err);
        this.#deliverFrame(socket, {
          error: err instanceof Error ? err : new Error(String(err)),
        });
      }
    });
  }

  #deliverFrame(socket: WebSocketLike, payload: FramePayload) {
    let waiters = frameWaiters.get(socket);
    if (waiters && waiters.length > 0) {
      const waiter = waiters.shift();
      waiter?.(payload);
      return;
    }
    let queue = frameQueue.get(socket);
    if (!queue) {
      queue = [];
      frameQueue.set(socket, queue);
    }
    queue.push(payload);
    debugLog('frame:queued', payload);
  }

  #attach(
    socket: WebSocketLike,
    event: string,
    handler: WebSocketEventHandler
  ): CleanupFn {
    if (typeof socket.addEventListener === 'function') {
      socket.addEventListener(event, handler);
      return () => socket.removeEventListener?.(event, handler);
    }

    if (typeof socket.on === 'function') {
      socket.on(event, handler);
      return () => {
        if (typeof socket.off === 'function') {
          socket.off(event, handler);
        } else if (typeof socket.removeListener === 'function') {
          socket.removeListener(event, handler);
        }
      };
    }

    throw new Error('WebSocket implementation must support events');
  }
}

function extractData(event: unknown): unknown {
  if (!event || typeof event !== 'object') {
    return event;
  }

  if ('data' in event) {
    return (event as { data?: unknown }).data;
  }

  return event;
}

function normalizePayload(raw: unknown): string {
  if (typeof raw === 'string') {
    return raw;
  }

  if (raw instanceof ArrayBuffer) {
    return decodeBytes(new Uint8Array(raw));
  }

  if (ArrayBuffer.isView(raw)) {
    const view = raw as ArrayBufferView;
    const bytes = new Uint8Array(
      view.buffer,
      view.byteOffset,
      view.byteLength
    );
    return decodeBytes(bytes);
  }

  if (
    typeof Buffer !== 'undefined' &&
    Buffer.isBuffer &&
    Buffer.isBuffer(raw)
  ) {
    return raw.toString('utf8');
  }

  if (raw == null) {
    return '';
  }

  return String(raw);
}

function decodeBytes(bytes: Uint8Array): string {
  if (textDecoder) {
    return textDecoder.decode(bytes);
  }
  if (typeof Buffer !== 'undefined') {
    return Buffer.from(bytes).toString('utf8');
  }
  let result = '';
  for (let i = 0; i < bytes.length; i += 1) {
    result += String.fromCharCode(bytes[i]);
  }
  return result;
}

export default SimpleDatastoreClient;
