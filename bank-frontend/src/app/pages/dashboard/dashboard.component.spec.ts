import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { provideRouter } from '@angular/router';
import { LOCALE_ID } from '@angular/core';
import { registerLocaleData } from '@angular/common';
import localePt from '@angular/common/locales/pt';
import { of } from 'rxjs';

import { DashboardComponent } from './dashboard.component';
import { AccountService } from '../../services/account.service';
import { AuthService } from '../../services/auth.service';

registerLocaleData(localePt, 'pt-BR');

describe('DashboardComponent', () => {
  let component: DashboardComponent;
  let fixture: ComponentFixture<DashboardComponent>;
  let accountServiceSpy: jasmine.SpyObj<AccountService>;

  beforeEach(async () => {
    accountServiceSpy = jasmine.createSpyObj('AccountService', ['getBalance']);
    accountServiceSpy.getBalance.and.returnValue(of({ accountId: 'account-1', balance: 1234.56 }));

    await TestBed.configureTestingModule({
      imports: [DashboardComponent],
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        provideRouter([]),
        { provide: AccountService, useValue: accountServiceSpy },
        { provide: LOCALE_ID, useValue: 'pt-BR' }
      ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(DashboardComponent);
    component = fixture.componentInstance;

    const authService = TestBed.inject(AuthService);
    spyOn(authService, 'getAccountId').and.returnValue('account-1');
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('displays the balance returned by AccountService', () => {
    fixture.detectChanges();

    expect(accountServiceSpy.getBalance).toHaveBeenCalledWith('account-1');
    expect(component.balance).toBe(1234.56);

    const balanceEl: HTMLElement = fixture.nativeElement.querySelector('.balance-value');
    expect(balanceEl.textContent).toContain('1.234,56');
  });

  it('reloads the balance when the refresh button is clicked', () => {
    fixture.detectChanges();
    accountServiceSpy.getBalance.calls.reset();

    const refreshButton: HTMLButtonElement = fixture.nativeElement.querySelector('.balance-card button');
    refreshButton.click();

    expect(accountServiceSpy.getBalance).toHaveBeenCalledTimes(1);
  });
});
