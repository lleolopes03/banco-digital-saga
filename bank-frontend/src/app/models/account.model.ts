export interface BalanceResponse {
  accountId: string;
  balance: number;
}

export type TransactionType = 'DEPOSIT' | 'TRANSFER_OUT' | 'TRANSFER_IN' | 'REVERSAL';

export interface TransactionResponse {
  id: string;
  accountId: string;
  type: TransactionType;
  amount: number;
  balanceAfter: number;
  relatedTransactionId: string | null;
  description: string;
  createdAt: string;
}

export interface PageResponse<T> {
  conteudo: T[];
  paginaAtual: number;
  totalPaginas: number;
  totalElementos: number;
  tamanhoPagina: number;
  primeira: boolean;
  ultima: boolean;
}

export interface StatementFilter {
  de?: string;
  ate?: string;
  pagina?: number;
  tamanhoPagina?: number;
}
