import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

import { AuthService } from './auth.service';
import { LoginResponse } from '../models/auth.model';

describe('AuthService', () => {
  let service: AuthService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule]
    });
    service = TestBed.inject(AuthService);
    httpMock = TestBed.inject(HttpTestingController);
    localStorage.clear();
  });

  afterEach(() => {
    httpMock.verify();
    localStorage.clear();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('stores the token and account id on successful login', () => {
    const mockResponse: LoginResponse = {
      token: 'jwt-token-abc',
      accountId: 'account-123',
      expiresInSeconds: 900
    };

    service.login('user@example.com', 'senha123').subscribe(response => {
      expect(response).toEqual(mockResponse);
    });

    const req = httpMock.expectOne('/api/auth/login');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({ email: 'user@example.com', senha: 'senha123' });
    req.flush(mockResponse);

    expect(service.isAuthenticated()).toBeTrue();
    expect(service.getToken()).toBe('jwt-token-abc');
    expect(service.getAccountId()).toBe('account-123');
  });

  it('does not store any token when login fails', () => {
    service.login('user@example.com', 'wrong-password').subscribe({
      next: () => fail('expected an error'),
      error: (error) => expect(error.status).toBe(401)
    });

    const req = httpMock.expectOne('/api/auth/login');
    req.flush({ message: 'Credenciais inválidas' }, { status: 401, statusText: 'Unauthorized' });

    expect(service.isAuthenticated()).toBeFalse();
    expect(service.getToken()).toBeNull();
    expect(service.getAccountId()).toBeNull();
  });

  it('clears the stored session on logout', () => {
    localStorage.setItem('bank_token', 'jwt-token-abc');
    localStorage.setItem('bank_account_id', 'account-123');

    service.logout().subscribe();

    const req = httpMock.expectOne('/api/auth/logout');
    expect(req.request.method).toBe('POST');
    req.flush(null, { status: 204, statusText: 'No Content' });

    expect(service.isAuthenticated()).toBeFalse();
    expect(service.getToken()).toBeNull();
  });
});
