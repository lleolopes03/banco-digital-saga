import { Component, inject, OnInit } from '@angular/core';
import { CurrencyPipe, DatePipe } from '@angular/common';
import { FormBuilder, ReactiveFormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { AccountService } from '../../services/account.service';
import { AuthService } from '../../services/auth.service';
import { TransactionResponse, TransactionType } from '../../models/account.model';

const TYPE_META: Record<TransactionType, { label: string; icon: string; cssClass: string }> = {
  DEPOSIT: { label: 'Depósito', icon: '↓', cssClass: 'type-deposit' },
  TRANSFER_IN: { label: 'Transferência recebida', icon: '↓', cssClass: 'type-in' },
  TRANSFER_OUT: { label: 'Transferência enviada', icon: '↑', cssClass: 'type-out' },
  REVERSAL: { label: 'Estorno', icon: '↺', cssClass: 'type-reversal' }
};

@Component({
  selector: 'app-statement',
  imports: [CurrencyPipe, DatePipe, ReactiveFormsModule, RouterLink],
  templateUrl: './statement.component.html',
  styleUrl: './statement.component.css'
})
export class StatementComponent implements OnInit {

  private formBuilder = inject(FormBuilder);
  private accountService = inject(AccountService);
  private authService = inject(AuthService);

  transactions: TransactionResponse[] = [];
  isLoading = false;
  errorMessage: string | null = null;

  currentPage = 0;
  pageSize = 20;
  totalPages = 0;
  totalElements = 0;
  isFirstPage = true;
  isLastPage = true;

  filterForm = this.formBuilder.group({
    de: [''],
    ate: ['']
  });

  ngOnInit(): void {
    this.loadStatement();
  }

  applyFilter(): void {
    this.currentPage = 0;
    this.loadStatement();
  }

  clearFilter(): void {
    this.filterForm.reset({ de: '', ate: '' });
    this.currentPage = 0;
    this.loadStatement();
  }

  goToPage(page: number): void {
    if (page < 0 || page >= this.totalPages) {
      return;
    }
    this.currentPage = page;
    this.loadStatement();
  }

  loadStatement(): void {
    const accountId = this.authService.getAccountId();
    if (!accountId) {
      return;
    }

    const { de, ate } = this.filterForm.getRawValue();

    this.isLoading = true;
    this.errorMessage = null;

    this.accountService.getStatement(accountId, {
      de: de || undefined,
      ate: ate || undefined,
      pagina: this.currentPage,
      tamanhoPagina: this.pageSize
    }).subscribe({
      next: page => {
        this.transactions = page.conteudo;
        this.currentPage = page.paginaAtual;
        this.totalPages = page.totalPaginas;
        this.totalElements = page.totalElementos;
        this.isFirstPage = page.primeira;
        this.isLastPage = page.ultima;
        this.isLoading = false;
      },
      error: () => {
        this.isLoading = false;
        this.errorMessage = 'Não foi possível carregar o extrato.';
      }
    });
  }

  metaFor(type: TransactionType) {
    return TYPE_META[type];
  }
}
