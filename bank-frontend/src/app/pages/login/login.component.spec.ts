import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { provideRouter, Router } from '@angular/router';

import { LoginComponent } from './login.component';

describe('LoginComponent', () => {
  let component: LoginComponent;
  let fixture: ComponentFixture<LoginComponent>;
  let httpMock: HttpTestingController;
  let router: Router;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [LoginComponent],
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        provideRouter([])
      ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(LoginComponent);
    component = fixture.componentInstance;
    httpMock = TestBed.inject(HttpTestingController);
    router = TestBed.inject(Router);
    fixture.detectChanges();
  });

  afterEach(() => httpMock.verify());

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('does not submit an invalid form', () => {
    component.form.setValue({ email: 'not-an-email', senha: '123' });

    component.onSubmit();

    httpMock.expectNone('/api/auth/login');
    expect(component.form.touched).toBeTrue();
  });

  it('navigates to /dashboard on successful login', () => {
    spyOn(router, 'navigate');
    component.form.setValue({ email: 'user@example.com', senha: 'senha123' });

    component.onSubmit();

    const req = httpMock.expectOne('/api/auth/login');
    req.flush({ token: 'jwt-token', accountId: 'account-1', expiresInSeconds: 900 });

    expect(router.navigate).toHaveBeenCalledWith(['/dashboard']);
    expect(component.errorMessage).toBeNull();
  });

  it('shows an error message when credentials are invalid', () => {
    spyOn(router, 'navigate');
    component.form.setValue({ email: 'user@example.com', senha: 'senha-errada' });

    component.onSubmit();

    const req = httpMock.expectOne('/api/auth/login');
    req.flush({ message: 'Credenciais inválidas' }, { status: 401, statusText: 'Unauthorized' });

    expect(component.errorMessage).toBe('E-mail ou senha inválidos.');
    expect(router.navigate).not.toHaveBeenCalled();
  });
});
