import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { BalanceResponse, PageResponse, StatementFilter, TransactionResponse } from '../models/account.model';

@Injectable({
  providedIn: 'root'
})
export class AccountService {
  private http = inject(HttpClient);

  getBalance(accountId: string): Observable<BalanceResponse> {
    return this.http.get<BalanceResponse>(`/api/accounts/${accountId}/balance`);
  }

  getStatement(accountId: string, filter: StatementFilter): Observable<PageResponse<TransactionResponse>> {
    let params = new HttpParams()
      .set('pagina', String(filter.pagina ?? 0))
      .set('tamanhoPagina', String(filter.tamanhoPagina ?? 20));

    if (filter.de) {
      params = params.set('de', filter.de);
    }
    if (filter.ate) {
      params = params.set('ate', filter.ate);
    }

    return this.http.get<PageResponse<TransactionResponse>>(`/api/accounts/${accountId}/statement`, { params });
  }
}
