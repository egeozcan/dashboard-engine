/** Controllable WebSocket double for Signal client tests. */
export class MockWebSocket {
  static instances: MockWebSocket[] = [];
  static readonly OPEN = 1;
  readonly url: string;
  readyState = 0;
  sent: Array<Record<string, unknown>> = [];
  onopen: ((ev: Event) => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  onclose: ((ev: CloseEvent) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;

  constructor(url: string) {
    this.url = url;
    MockWebSocket.instances.push(this);
  }

  static reset() {
    MockWebSocket.instances = [];
  }

  static latest(): MockWebSocket {
    const ws = MockWebSocket.instances[MockWebSocket.instances.length - 1];
    if (!ws) throw new Error("no MockWebSocket created");
    return ws;
  }

  send(data: string) {
    this.sent.push(JSON.parse(data));
  }

  close() {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.onclose?.(new Event("close") as CloseEvent);
  }

  // ---- test controls ----
  open() {
    this.readyState = 1;
    this.onopen?.(new Event("open"));
  }

  receive(msg: unknown) {
    this.onmessage?.(new MessageEvent("message", { data: JSON.stringify(msg) }));
  }

  drop() {
    this.readyState = 3;
    this.onclose?.(new Event("close") as CloseEvent);
  }
}

/** WebSocket that never connects (default in tests so views do not dial the network). */
export class InertWebSocket {
  readyState = 0;
  onopen: unknown = null;
  onmessage: unknown = null;
  onclose: unknown = null;
  onerror: unknown = null;
  constructor(_url: string) {}
  send() {}
  close() {}
}
