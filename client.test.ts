import { describe, test, expect } from 'vitest'
import { EventEmitter } from 'events'
import SimpleDatastoreClient from './client'

type ConnectHandler = (ctx: {
  url: string
  socket: MockWebSocket
  sendServerMessage: (payload: unknown) => void
  interceptClientMessage: (handler: (data: string) => void) => void
}) => void

class MockWebSocket extends EventEmitter {
  public readonly OPEN = 1
  public readonly CLOSED = 3
  public readyState = 0
  private sendInterceptor: ((data: string) => void) | null = null

  constructor(
    public readonly url: string,
    private readonly onConnect?: ConnectHandler
  ) {
    super()
    queueMicrotask(() => {
      this.readyState = this.OPEN
      this.emit('open')
      this.onConnect?.({
        url: this.url,
        socket: this,
        sendServerMessage: (payload: unknown) => {
          this.emit('message', { data: JSON.stringify(payload) })
        },
        interceptClientMessage: (handler: (data: string) => void) => {
          this.sendInterceptor = handler
        }
      })
    })
  }

  send(data: string) {
    this.sendInterceptor?.(data)
  }

  close() {
    if (this.readyState === this.CLOSED) return
    this.readyState = this.CLOSED
    this.emit('close')
  }

  addEventListener(event: string, handler: (...args: unknown[]) => void) {
    this.on(event, handler)
  }

  removeEventListener(event: string, handler: (...args: unknown[]) => void) {
    this.off(event, handler)
  }
}

function createMockWebSocketFactory(handler?: ConnectHandler) {
  return class extends MockWebSocket {
    constructor(url: string) {
      super(url, handler)
    }
  }
}

describe('SimpleDatastoreClient', () => {
  test('pull sends since parameter and returns entities', async () => {
    const WebSocketClass = createMockWebSocketFactory(({ url, sendServerMessage }) => {
      const parsed = new URL(url)
      expect(parsed.searchParams.get('since')).toBe('1700000000')
      expect(parsed.searchParams.get('token')).toBe('test-token')
      sendServerMessage([
        'RESULT',
        [{ id: 'one', content: 'hello', last_change: 1700000001 }]
      ])
    })

    const client = new SimpleDatastoreClient({
      baseUrl: 'ws://localhost:4000/ws',
      token: 'test-token',
      WebSocketClass
    })

    const rows = await client.pull({ since: 1700000000 })
    expect(rows).toEqual([{ id: 'one', content: 'hello', last_change: 1700000001 }])
  })

  test('push streams EVENT frames and awaits acknowledgements', async () => {
    const seenFrames: unknown[] = []
    const WebSocketClass = createMockWebSocketFactory(({ interceptClientMessage, sendServerMessage }) => {
      interceptClientMessage((raw) => {
        const frame = JSON.parse(raw)
        seenFrames.push(frame)
        const [, entityId] = frame
        sendServerMessage(['OK', entityId, true, null])
      })
    })

    const client = new SimpleDatastoreClient({
      baseUrl: 'ws://localhost:4000/ws',
      token: 'push-token',
      WebSocketClass
    })

    const acknowledgements = await client.push([
      { id: 'alpha', payload: { content: 'first' } },
      { id: 'beta', payload: { content: 'second' } }
    ])

    expect(seenFrames).toEqual([
      ['EVENT', 'alpha', { content: 'first' }],
      ['EVENT', 'beta', { content: 'second' }]
    ])
    expect(acknowledgements).toEqual([
      { id: 'alpha', success: true, error: null },
      { id: 'beta', success: true, error: null }
    ])
  })

  test('sync handles initial RESULT and subsequent EVENT acknowledgements', async () => {
    const WebSocketClass = createMockWebSocketFactory(({ sendServerMessage, interceptClientMessage }) => {
      sendServerMessage([
        'RESULT',
        [{ id: 'seed', content: 'existing', last_change: 10 }]
      ])
      interceptClientMessage((raw) => {
        const [, entityId] = JSON.parse(raw)
        sendServerMessage(['OK', entityId, entityId === 'new', null])
      })
    })

    const client = new SimpleDatastoreClient({
      baseUrl: 'ws://localhost:4000/ws',
      token: 'sync-token',
      WebSocketClass
    })

    const { result, acknowledgements } = await client.sync({
      since: 5,
      events: [
        { id: 'new', payload: { content: 'create' } },
        { id: 'old', payload: { content: 'update' } }
      ]
    })

    expect(result).toEqual([{ id: 'seed', content: 'existing', last_change: 10 }])
    expect(acknowledgements).toEqual([
      { id: 'new', success: true, error: null },
      { id: 'old', success: false, error: null }
    ])
  })
})
