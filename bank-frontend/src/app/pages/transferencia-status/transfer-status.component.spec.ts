import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { ActivatedRoute, convertToParamMap, provideRouter } from '@angular/router';
import { Subject, of } from 'rxjs';

import { TransferStatusComponent } from './transfer-status.component';
import { TransferStatusService } from '../../services/transfer-status.service';
import { TransferService } from '../../services/transfer.service';
import { SagaStatusUpdate } from '../../models/transfer.model';

describe('TransferStatusComponent', () => {
  let fixture: ComponentFixture<TransferStatusComponent>;
  let component: TransferStatusComponent;
  let updates$: Subject<SagaStatusUpdate>;
  let transferStatusServiceSpy: jasmine.SpyObj<TransferStatusService>;
  let transferServiceSpy: jasmine.SpyObj<TransferService>;

  function setup(): void {
    fixture = TestBed.createComponent(TransferStatusComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  }

  beforeEach(() => {
    updates$ = new Subject<SagaStatusUpdate>();
    transferStatusServiceSpy = jasmine.createSpyObj('TransferStatusService', ['connect']);
    transferStatusServiceSpy.connect.and.returnValue(updates$.asObservable());
    transferServiceSpy = jasmine.createSpyObj('TransferService', ['getTransfer']);

    TestBed.configureTestingModule({
      imports: [TransferStatusComponent],
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        provideRouter([]),
        { provide: TransferStatusService, useValue: transferStatusServiceSpy },
        { provide: TransferService, useValue: transferServiceSpy },
        {
          provide: ActivatedRoute,
          useValue: { snapshot: { paramMap: convertToParamMap({ transferId: 'transfer-1' }) } }
        }
      ]
    });
  });

  it('starts every step as pending before the first update arrives', () => {
    setup();
    expect(component.steps.every(step => step.state === 'pending')).toBeTrue();
    expect(transferStatusServiceSpy.connect).toHaveBeenCalledWith('transfer-1');
  });

  it('marks earlier steps done and the current one active as the saga advances', () => {
    setup();

    updates$.next({ transferId: 'transfer-1', status: 'ANTIFRAUD_PENDING', failureReason: null, updatedAt: 't' });
    expect(component.steps.map(s => s.state)).toEqual(['done', 'active', 'pending', 'pending', 'pending']);

    updates$.next({ transferId: 'transfer-1', status: 'DEBITED', failureReason: null, updatedAt: 't' });
    expect(component.steps.map(s => s.state)).toEqual(['done', 'done', 'done', 'active', 'pending']);
  });

  it('marks all steps done on COMPLETED and shows the success banner', () => {
    setup();

    updates$.next({ transferId: 'transfer-1', status: 'COMPLETED', failureReason: null, updatedAt: 't' });

    expect(component.steps.every(step => step.state === 'done')).toBeTrue();
    expect(component.isTerminalSuccess).toBeTrue();
    expect(component.isTerminalFailure).toBeFalse();
  });

  it('marks the antifraud step as error and shows the failure reason on rejection/compensation', () => {
    setup();

    updates$.next({ transferId: 'transfer-1', status: 'ANTIFRAUD_PENDING', failureReason: null, updatedAt: 't' });
    updates$.next({ transferId: 'transfer-1', status: 'ANTIFRAUD_REJECTED', failureReason: 'LIMITE_EXCEDIDO', updatedAt: 't' });

    expect(component.steps.map(s => s.state)).toEqual(['done', 'error', 'pending', 'pending', 'pending']);

    updates$.next({ transferId: 'transfer-1', status: 'FAILED', failureReason: 'LIMITE_EXCEDIDO', updatedAt: 't' });

    expect(component.steps.map(s => s.state)).toEqual(['done', 'error', 'pending', 'pending', 'pending']);
    expect(component.isTerminalFailure).toBeTrue();
    expect(component.latest?.failureReason).toBe('LIMITE_EXCEDIDO');
  });

  it('marks the credit step as error when the failure happens after debit', () => {
    setup();

    updates$.next({ transferId: 'transfer-1', status: 'DEBITED', failureReason: null, updatedAt: 't' });
    updates$.next({ transferId: 'transfer-1', status: 'DEBITED', failureReason: 'CONTA_DESTINO_BLOQUEADA', updatedAt: 't' });

    expect(component.steps.map(s => s.state)).toEqual(['done', 'done', 'done', 'error', 'pending']);
  });

  it('falls back to polling GET /api/transfers/{id} when the WebSocket errors out', () => {
    setup();
    transferServiceSpy.getTransfer.and.returnValue(of({
      transferId: 'transfer-1',
      accountOrigemId: 'a',
      accountDestinoId: 'b',
      valor: 10,
      status: 'COMPLETED',
      failureReason: null
    }));

    updates$.error(new Error('WebSocket dropped'));

    expect(component.connectionMode).toBe('polling');
    expect(transferServiceSpy.getTransfer).toHaveBeenCalledWith('transfer-1');
    expect(component.isTerminalSuccess).toBeTrue();
  });
});
