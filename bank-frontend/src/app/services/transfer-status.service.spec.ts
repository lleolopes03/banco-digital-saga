import { TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';

import { TransferStatusService } from './transfer-status.service';
import { AuthService } from './auth.service';
import { SagaStatusUpdate } from '../models/transfer.model';

interface FakeClient {
  brokerURL: string;
  connectHeaders: Record<string, string>;
  reconnectDelay: number;
  onConnect: () => void;
  onStompError: (frame: unknown) => void;
  onWebSocketError: () => void;
  activate: jasmine.Spy;
  deactivate: jasmine.Spy;
  subscribe: jasmine.Spy;
}

function createFakeClient(): FakeClient {
  return {
    brokerURL: '',
    connectHeaders: {},
    reconnectDelay: 0,
    onConnect: () => {},
    onStompError: () => {},
    onWebSocketError: () => {},
    activate: jasmine.createSpy('activate'),
    deactivate: jasmine.createSpy('deactivate'),
    subscribe: jasmine.createSpy('subscribe')
  };
}

describe('TransferStatusService', () => {
  let service: TransferStatusService;
  let fakeClient: FakeClient;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [provideHttpClient(), provideHttpClientTesting()]
    });
    service = TestBed.inject(TransferStatusService);

    fakeClient = createFakeClient();
    spyOn(service as unknown as { createClient: () => unknown }, 'createClient').and.returnValue(fakeClient);
  });

  it('activates the STOMP client with the JWT in the connect headers', () => {
    const authService = TestBed.inject(AuthService);
    spyOn(authService, 'getToken').and.returnValue('jwt-token-abc');

    service.connect('transfer-1').subscribe();

    expect(fakeClient.activate).toHaveBeenCalled();
    expect(fakeClient.connectHeaders).toEqual({ Authorization: 'Bearer jwt-token-abc' });
  });

  it('subscribes to /topic/transfers/{id} and emits updates in the order received', () => {
    const received: SagaStatusUpdate[] = [];
    let completed = false;

    service.connect('transfer-1').subscribe({
      next: update => received.push(update),
      complete: () => (completed = true)
    });

    fakeClient.onConnect();

    expect(fakeClient.subscribe).toHaveBeenCalledWith('/topic/transfers/transfer-1', jasmine.any(Function));
    const messageHandler = fakeClient.subscribe.calls.mostRecent().args[1];

    const update1: SagaStatusUpdate = { transferId: 'transfer-1', status: 'ANTIFRAUD_PENDING', failureReason: null, updatedAt: '2026-07-20T10:00:00Z' };
    const update2: SagaStatusUpdate = { transferId: 'transfer-1', status: 'DEBITED', failureReason: null, updatedAt: '2026-07-20T10:00:01Z' };
    const update3: SagaStatusUpdate = { transferId: 'transfer-1', status: 'COMPLETED', failureReason: null, updatedAt: '2026-07-20T10:00:02Z' };

    messageHandler({ body: JSON.stringify(update1) });
    messageHandler({ body: JSON.stringify(update2) });
    messageHandler({ body: JSON.stringify(update3) });

    expect(received).toEqual([update1, update2, update3]);
    expect(completed).toBeTrue();
  });

  it('completes the observable when a FAILED status arrives', () => {
    let completed = false;
    service.connect('transfer-2').subscribe({ complete: () => (completed = true) });

    fakeClient.onConnect();
    const messageHandler = fakeClient.subscribe.calls.mostRecent().args[1];

    messageHandler({
      body: JSON.stringify({ transferId: 'transfer-2', status: 'FAILED', failureReason: 'LIMITE_EXCEDIDO', updatedAt: '2026-07-20T10:00:00Z' })
    });

    expect(completed).toBeTrue();
  });

  it('deactivates the client when unsubscribed', () => {
    const subscription = service.connect('transfer-3').subscribe();
    subscription.unsubscribe();

    expect(fakeClient.deactivate).toHaveBeenCalled();
  });

  it('propagates a STOMP error to the observable', () => {
    let receivedError: Error | undefined;
    service.connect('transfer-4').subscribe({ error: err => (receivedError = err) });

    fakeClient.onStompError({ headers: { message: 'boom' } });

    expect(receivedError?.message).toBe('boom');
  });
});
