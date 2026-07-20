import { TestBed } from '@angular/core/testing';
import { provideRouter, Router } from '@angular/router';
import { ActivatedRouteSnapshot, RouterStateSnapshot } from '@angular/router';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';

import { authGuard } from './auth.guard';
import { AuthService } from '../services/auth.service';

describe('authGuard', () => {
  let router: Router;

  const runGuard = () =>
    TestBed.runInInjectionContext(() =>
      authGuard({} as ActivatedRouteSnapshot, {} as RouterStateSnapshot)
    );

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [provideRouter([]), provideHttpClient(), provideHttpClientTesting()]
    });
    router = TestBed.inject(Router);
    localStorage.clear();
  });

  afterEach(() => localStorage.clear());

  it('blocks access and redirects to /login when there is no token', () => {
    spyOn(router, 'navigate');

    const result = runGuard();

    expect(result).toBeFalse();
    expect(router.navigate).toHaveBeenCalledWith(['/login']);
  });

  it('allows access when a valid token is stored', () => {
    localStorage.setItem('bank_token', 'jwt-token-abc');
    spyOn(router, 'navigate');

    const result = runGuard();

    expect(result).toBeTrue();
    expect(router.navigate).not.toHaveBeenCalled();
  });

  it('injects AuthService via the injection context', () => {
    localStorage.setItem('bank_token', 'jwt-token-abc');
    const authService = TestBed.inject(AuthService);

    expect(authService.isAuthenticated()).toBeTrue();
    expect(runGuard()).toBeTrue();
  });
});
