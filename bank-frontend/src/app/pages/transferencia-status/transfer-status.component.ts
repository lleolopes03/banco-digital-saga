import { Component, inject, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { Subscription, interval } from 'rxjs';
import { startWith, switchMap, takeWhile } from 'rxjs/operators';
import { TransferStatusService } from '../../services/transfer-status.service';
import { TransferService } from '../../services/transfer.service';
import { SagaStatus, SagaStatusUpdate, TERMINAL_SAGA_STATUSES } from '../../models/transfer.model';

type StepState = 'pending' | 'active' | 'done' | 'error';

interface Step {
  label: string;
  state: StepState;
}

const STEP_LABELS = [
  'Reservando valor',
  'Validando antifraude',
  'Confirmando débito',
  'Creditando destinatário',
  'Concluída'
];

// Índice do passo alcançado em cada status "feliz" da saga (sem falha).
const HAPPY_STEP_INDEX: Partial<Record<SagaStatus, number>> = {
  STARTED: 0,
  RESERVED: 0,
  ANTIFRAUD_PENDING: 1,
  ANTIFRAUD_APPROVED: 2,
  DEBITED: 3,
  CREDITED: 4,
  COMPLETED: 4
};

// Em qual passo a falha aconteceu, para os status em que ela pode ser detectada
// (ANTIFRAUD_REJECTED sempre falha no passo antifraude; DEBITED com failureReason preenchido
// significa que o crédito no destino falhou e a compensação está em voo).
const FAILURE_STEP_INDEX: Partial<Record<SagaStatus, number>> = {
  ANTIFRAUD_REJECTED: 1,
  DEBITED: 3
};

@Component({
  selector: 'app-transfer-status',
  imports: [RouterLink],
  templateUrl: './transfer-status.component.html',
  styleUrl: './transfer-status.component.css'
})
export class TransferStatusComponent implements OnInit, OnDestroy {

  private route = inject(ActivatedRoute);
  private transferStatusService = inject(TransferStatusService);
  private transferService = inject(TransferService);

  transferId = '';
  latest: SagaStatusUpdate | null = null;
  connectionMode: 'websocket' | 'polling' = 'websocket';

  private erroredStep: number | null = null;
  private wsSubscription?: Subscription;
  private pollingSubscription?: Subscription;

  ngOnInit(): void {
    this.transferId = this.route.snapshot.paramMap.get('transferId')!;
    this.connectViaWebSocket();
  }

  ngOnDestroy(): void {
    this.wsSubscription?.unsubscribe();
    this.pollingSubscription?.unsubscribe();
  }

  get steps(): Step[] {
    if (!this.latest) {
      return STEP_LABELS.map(label => ({ label, state: 'pending' }));
    }

    const currentIndex = HAPPY_STEP_INDEX[this.latest.status] ?? this.erroredStep ?? 0;
    const isCompleted = this.latest.status === 'COMPLETED';

    return STEP_LABELS.map((label, index) => {
      if (this.erroredStep !== null && index === this.erroredStep) {
        return { label, state: 'error' };
      }
      if (this.erroredStep !== null && index > this.erroredStep) {
        return { label, state: 'pending' };
      }
      if (index < currentIndex || (index === currentIndex && isCompleted)) {
        return { label, state: 'done' };
      }
      if (index === currentIndex) {
        return { label, state: 'active' };
      }
      return { label, state: 'pending' };
    });
  }

  get isTerminalFailure(): boolean {
    return this.latest?.status === 'FAILED';
  }

  get isTerminalSuccess(): boolean {
    return this.latest?.status === 'COMPLETED';
  }

  private connectViaWebSocket(): void {
    this.connectionMode = 'websocket';
    this.wsSubscription = this.transferStatusService.connect(this.transferId).subscribe({
      next: update => this.applyUpdate(update),
      error: () => this.fallbackToPolling(),
      complete: () => {
        if (!this.latest || !TERMINAL_SAGA_STATUSES.includes(this.latest.status)) {
          this.fallbackToPolling();
        }
      }
    });
  }

  private fallbackToPolling(): void {
    if (this.pollingSubscription) {
      return;
    }
    this.connectionMode = 'polling';
    this.pollingSubscription = interval(2000).pipe(
      startWith(0),
      switchMap(() => this.transferService.getTransfer(this.transferId)),
      takeWhile(response => !TERMINAL_SAGA_STATUSES.includes(response.status), true)
    ).subscribe(response => {
      this.applyUpdate({
        transferId: response.transferId,
        status: response.status,
        failureReason: response.failureReason,
        updatedAt: new Date().toISOString()
      });
    });
  }

  private applyUpdate(update: SagaStatusUpdate): void {
    this.latest = update;
    if (update.failureReason && this.erroredStep === null) {
      this.erroredStep = FAILURE_STEP_INDEX[update.status] ?? HAPPY_STEP_INDEX[update.status] ?? 0;
    }
  }
}
