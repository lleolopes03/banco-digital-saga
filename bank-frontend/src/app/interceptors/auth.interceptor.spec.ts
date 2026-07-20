import { TestBed } from '@angular/core/testing';
import { HttpClient, provideHttpClient, withInterceptors } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { provideRouter, Router } from '@angular/router';

import { authInterceptor } from './auth.interceptor';
import { AuthService } from '../services/auth.service';

describe('authInterceptor', () => {
  let httpClient: HttpClient;
  let httpMock: HttpTestingController;
  let authService: AuthService;
  let router: Router;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(withInterceptors([authInterceptor])),
        provideHttpClientTesting(),
        provideRouter([])
      ]
    });

    httpClient = TestBed.inject(HttpClient);
    httpMock = TestBed.inject(HttpTestingController);
    authService = TestBed.inject(AuthService);
    router = TestBed.inject(Router);
    localStorage.clear();
  });

  afterEach(() => {
    httpMock.verify();
    localStorage.clear();
  });

  it('injects the Authorization header when a token is stored', () => {
    localStorage.setItem('bank_token', 'jwt-token-abc');

    httpClient.get('/api/accounts/123/balance').subscribe();

    const req = httpMock.expectOne('/api/accounts/123/balance');
    expect(req.request.headers.get('Authorization')).toBe('Bearer jwt-token-abc');
    req.flush({});
  });

  it('does not inject the header when there is no token', () => {
    httpClient.get('/api/accounts/123/balance').subscribe();

    const req = httpMock.expectOne('/api/accounts/123/balance');
    expect(req.request.headers.has('Authorization')).toBeFalse();
    req.flush({});
  });

  it('logs out and redirects to /login on a 401 response', () => {
    localStorage.setItem('bank_token', 'jwt-token-abc');
    localStorage.setItem('bank_account_id', 'account-123');
    spyOn(router, 'navigate');

    httpClient.get('/api/accounts/123/balance').subscribe({
      next: () => fail('expected an error'),
      error: () => {}
    });

    const req = httpMock.expectOne('/api/accounts/123/balance');
    req.flush({ message: 'Unauthorized' }, { status: 401, statusText: 'Unauthorized' });

    expect(authService.isAuthenticated()).toBeFalse();
    expect(router.navigate).toHaveBeenCalledWith(['/login']);
  });
});
