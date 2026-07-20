import { Component, OnInit, inject } from '@angular/core';
import { CurrencyPipe } from '@angular/common';
import { Router, RouterLink } from '@angular/router';
import { AccountService } from '../../services/account.service';
import { AuthService } from '../../services/auth.service';

@Component({
  selector: 'app-dashboard',
  imports: [CurrencyPipe, RouterLink],
  templateUrl: './dashboard.component.html',
  styleUrl: './dashboard.component.css'
})
export class DashboardComponent implements OnInit {
  private accountService = inject(AccountService);
  private authService = inject(AuthService);
  private router = inject(Router);

  balance: number | null = null;
  isLoading = false;
  errorMessage: string | null = null;

  ngOnInit(): void {
    this.loadBalance();
  }

  loadBalance(): void {
    const accountId = this.authService.getAccountId();
    if (!accountId) {
      return;
    }

    this.isLoading = true;
    this.errorMessage = null;

    this.accountService.getBalance(accountId).subscribe({
      next: response => {
        this.balance = response.balance;
        this.isLoading = false;
      },
      error: () => {
        this.isLoading = false;
        this.errorMessage = 'Não foi possível carregar o saldo.';
      }
    });
  }

  logout(): void {
    this.authService.logout().subscribe({
      next: () => this.router.navigate(['/login']),
      error: () => this.router.navigate(['/login'])
    });
  }
}
