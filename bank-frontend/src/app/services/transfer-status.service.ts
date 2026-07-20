import { Injectable, inject } from '@angular/core';
import { Client, IFrame, IMessage } from '@stomp/stompjs';
import { Observable, Subscriber } from 'rxjs';
import { AuthService } from './auth.service';
import { SagaStatusUpdate, TERMINAL_SAGA_STATUSES } from '../models/transfer.model';

@Injectable({
  providedIn: 'root'
})
export class TransferStatusService {
  private authService = inject(AuthService);

  connect(transferId: string): Observable<SagaStatusUpdate> {
    return new Observable<SagaStatusUpdate>(subscriber => {
      const client = this.createClient();

      client.brokerURL = this.buildBrokerUrl();
      client.connectHeaders = this.buildConnectHeaders();
      client.reconnectDelay = 0;

      client.onConnect = () => {
        client.subscribe(`/topic/transfers/${transferId}`, (message: IMessage) => {
          this.emitUpdate(message.body, subscriber);
        });
      };
      client.onStompError = (frame: IFrame) => {
        subscriber.error(new Error(frame.headers['message'] ?? 'Erro STOMP na conexão de acompanhamento.'));
      };
      client.onWebSocketError = () => {
        subscriber.error(new Error('Erro de conexão WebSocket.'));
      };

      client.activate();

      return () => client.deactivate();
    });
  }

  protected createClient(): Client {
    return new Client();
  }

  private emitUpdate(rawBody: string, subscriber: Subscriber<SagaStatusUpdate>): void {
    const update: SagaStatusUpdate = JSON.parse(rawBody);
    subscriber.next(update);
    if (TERMINAL_SAGA_STATUSES.includes(update.status)) {
      subscriber.complete();
    }
  }

  private buildBrokerUrl(): string {
    const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
    return `${protocol}://${window.location.host}/ws`;
  }

  private buildConnectHeaders(): Record<string, string> {
    const token = this.authService.getToken();
    return token ? { Authorization: `Bearer ${token}` } : {};
  }
}
