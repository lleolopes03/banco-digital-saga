import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { provideRouter } from '@angular/router';
import { of } from 'rxjs';

import { StatementComponent } from './statement.component';
import { AccountService } from '../../services/account.service';
import { AuthService } from '../../services/auth.service';
import { PageResponse, TransactionResponse } from '../../models/account.model';

describe('StatementComponent', () => {
  let component: StatementComponent;
  let fixture: ComponentFixture<StatementComponent>;
  let accountServiceSpy: jasmine.SpyObj<AccountService>;

  const emptyPage: PageResponse<TransactionResponse> = {
    conteudo: [],
    paginaAtual: 0,
    totalPaginas: 1,
    totalElementos: 0,
    tamanhoPagina: 20,
    primeira: true,
    ultima: true
  };

  beforeEach(async () => {
    accountServiceSpy = jasmine.createSpyObj('AccountService', ['getStatement']);
    accountServiceSpy.getStatement.and.returnValue(of(emptyPage));

    await TestBed.configureTestingModule({
      imports: [StatementComponent],
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        provideRouter([]),
        { provide: AccountService, useValue: accountServiceSpy }
      ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(StatementComponent);
    component = fixture.componentInstance;

    const authService = TestBed.inject(AuthService);
    spyOn(authService, 'getAccountId').and.returnValue('account-1');
  });

  it('should create and load the first page on init', () => {
    fixture.detectChanges();

    expect(component).toBeTruthy();
    expect(accountServiceSpy.getStatement).toHaveBeenCalledWith('account-1', {
      de: undefined,
      ate: undefined,
      pagina: 0,
      tamanhoPagina: 20
    });
  });

  it('sends the selected date range when the filter is applied', () => {
    fixture.detectChanges();
    accountServiceSpy.getStatement.calls.reset();

    component.filterForm.setValue({ de: '2026-07-01', ate: '2026-07-20' });
    component.applyFilter();

    expect(accountServiceSpy.getStatement).toHaveBeenCalledWith('account-1', {
      de: '2026-07-01',
      ate: '2026-07-20',
      pagina: 0,
      tamanhoPagina: 20
    });
  });

  it('requests the next page and keeps the current filter', () => {
    accountServiceSpy.getStatement.and.returnValue(of({ ...emptyPage, totalPaginas: 3, ultima: false }));
    fixture.detectChanges();
    accountServiceSpy.getStatement.calls.reset();
    accountServiceSpy.getStatement.and.returnValue(of({ ...emptyPage, paginaAtual: 1, totalPaginas: 3 }));

    component.goToPage(1);

    expect(accountServiceSpy.getStatement).toHaveBeenCalledWith('account-1', {
      de: undefined,
      ate: undefined,
      pagina: 1,
      tamanhoPagina: 20
    });
  });

  it('does not request a page outside the valid range', () => {
    fixture.detectChanges();
    accountServiceSpy.getStatement.calls.reset();

    component.goToPage(-1);
    component.goToPage(5);

    expect(accountServiceSpy.getStatement).not.toHaveBeenCalled();
  });
});
