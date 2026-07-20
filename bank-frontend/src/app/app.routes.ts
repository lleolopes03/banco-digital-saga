import { Routes } from '@angular/router';
import { LoginComponent } from './pages/login/login.component';
import { DashboardComponent } from './pages/dashboard/dashboard.component';
import { TransferenciaComponent } from './pages/transferencia/transferencia.component';
import { TransferStatusComponent } from './pages/transferencia-status/transfer-status.component';
import { StatementComponent } from './pages/extrato/statement.component';
import { authGuard } from './guards/auth.guard';

export const routes: Routes = [
  { path: '', redirectTo: 'login', pathMatch: 'full' },
  { path: 'login', component: LoginComponent },
  { path: 'dashboard', component: DashboardComponent, canActivate: [authGuard] },
  { path: 'extrato', component: StatementComponent, canActivate: [authGuard] },
  { path: 'transferencia', component: TransferenciaComponent, canActivate: [authGuard] },
  { path: 'transferencia/:transferId/status', component: TransferStatusComponent, canActivate: [authGuard] }
];
