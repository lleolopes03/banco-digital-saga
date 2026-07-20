import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { provideRouter, Router } from '@angular/router';

import { TransferenciaComponent } from './transferencia.component';
import { AuthService } from '../../services/auth.service';

describe('TransferenciaComponent', () => {
  let component: TransferenciaComponent;
  let fixture: ComponentFixture<TransferenciaComponent>;
  let httpMock: HttpTestingController;
  let router: Router;

  const OWN_ACCOUNT_ID = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
  const OTHER_ACCOUNT_ID = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb';

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TransferenciaComponent],
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        provideRouter([])
      ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(TransferenciaComponent);
    component = fixture.componentInstance;
    httpMock = TestBed.inject(HttpTestingController);
    router = TestBed.inject(Router);

    const authService = TestBed.inject(AuthService);
    spyOn(authService, 'getAccountId').and.returnValue(OWN_ACCOUNT_ID);

    fixture.detectChanges();
  });

  afterEach(() => httpMock.verify());

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('rejects a value that is not greater than zero', () => {
    component.form.setValue({ accountDestinoId: OTHER_ACCOUNT_ID, valor: 0, descricao: '' });
    expect(component.valor.valid).toBeFalse();
  });

  it('rejects a destination account equal to the origin account', () => {
    component.form.setValue({ accountDestinoId: OWN_ACCOUNT_ID, valor: 100, descricao: '' });
    expect(component.accountDestinoId.errors?.['sameAccount']).toBeTrue();
  });

  it('does not submit an invalid form', () => {
    component.form.setValue({ accountDestinoId: '', valor: null, descricao: '' });

    component.onSubmit();

    httpMock.expectNone('/api/transfers');
    expect(component.form.touched).toBeTrue();
  });

  it('submits a POST to /api/transfers and navigates to the status screen', () => {
    spyOn(router, 'navigate');
    component.form.setValue({ accountDestinoId: OTHER_ACCOUNT_ID, valor: 150.5, descricao: 'Aluguel' });

    component.onSubmit();

    const req = httpMock.expectOne('/api/transfers');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({
      accountOrigemId: OWN_ACCOUNT_ID,
      accountDestinoId: OTHER_ACCOUNT_ID,
      valor: 150.5
    });
    req.flush({ transferId: 'transfer-123' });

    expect(router.navigate).toHaveBeenCalledWith(['/transferencia', 'transfer-123', 'status']);
  });

  it('shows an error message when the request fails', () => {
    spyOn(router, 'navigate');
    component.form.setValue({ accountDestinoId: OTHER_ACCOUNT_ID, valor: 150.5, descricao: '' });

    component.onSubmit();

    const req = httpMock.expectOne('/api/transfers');
    req.flush({ message: 'Dados inválidos' }, { status: 400, statusText: 'Bad Request' });

    expect(component.errorMessage).toContain('Dados inválidos');
    expect(router.navigate).not.toHaveBeenCalled();
  });
});
