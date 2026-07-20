export type SagaStatus =
  | 'STARTED'
  | 'RESERVED'
  | 'ANTIFRAUD_PENDING'
  | 'ANTIFRAUD_APPROVED'
  | 'ANTIFRAUD_REJECTED'
  | 'DEBITED'
  | 'CREDITED'
  | 'COMPLETED'
  | 'FAILED';

export const TERMINAL_SAGA_STATUSES: readonly SagaStatus[] = ['COMPLETED', 'FAILED'];

export interface CreateTransferRequest {
  accountOrigemId: string;
  accountDestinoId: string;
  valor: number;
}

export interface TransferAcceptedResponse {
  transferId: string;
}

export interface TransferResponse {
  transferId: string;
  accountOrigemId: string;
  accountDestinoId: string;
  valor: number;
  status: SagaStatus;
  failureReason: string | null;
}

export interface SagaStatusUpdate {
  transferId: string;
  status: SagaStatus;
  failureReason: string | null;
  updatedAt: string;
}
