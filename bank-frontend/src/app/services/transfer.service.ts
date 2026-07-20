import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { CreateTransferRequest, TransferAcceptedResponse, TransferResponse } from '../models/transfer.model';

@Injectable({
  providedIn: 'root'
})
export class TransferService {
  private http = inject(HttpClient);

  createTransfer(request: CreateTransferRequest): Observable<TransferAcceptedResponse> {
    return this.http.post<TransferAcceptedResponse>('/api/transfers', request);
  }

  getTransfer(transferId: string): Observable<TransferResponse> {
    return this.http.get<TransferResponse>(`/api/transfers/${transferId}`);
  }
}
