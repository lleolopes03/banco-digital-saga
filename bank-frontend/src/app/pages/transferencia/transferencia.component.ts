import { Component, inject } from '@angular/core';
import { AbstractControl, FormBuilder, ReactiveFormsModule, ValidationErrors, ValidatorFn, Validators } from '@angular/forms';
import { Router, RouterLink } from '@angular/router';
import { HttpErrorResponse } from '@angular/common/http';
import { AuthService } from '../../services/auth.service';
import { TransferService } from '../../services/transfer.service';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function differentFromOwnAccountValidator(getOwnAccountId: () => string | null): ValidatorFn {
  return (control: AbstractControl): ValidationErrors | null => {
    const ownAccountId = getOwnAccountId();
    if (!control.value || !ownAccountId) {
      return null;
    }
    return control.value.trim().toLowerCase() === ownAccountId.toLowerCase() ? { sameAccount: true } : null;
  };
}

@Component({
  selector: 'app-transferencia',
  imports: [ReactiveFormsModule, RouterLink],
  templateUrl: './transferencia.component.html',
  styleUrl: './transferencia.component.css'
})
export class TransferenciaComponent {

  private formBuilder = inject(FormBuilder);
  private authService = inject(AuthService);
  private transferService = inject(TransferService);
  private router = inject(Router);

  errorMessage: string | null = null;
  isSubmitting = false;

  form = this.formBuilder.group({
    accountDestinoId: ['', [
      Validators.required,
      Validators.pattern(UUID_PATTERN),
      differentFromOwnAccountValidator(() => this.authService.getAccountId())
    ]],
    valor: [null as number | null, [Validators.required, Validators.min(0.01)]],
    descricao: ['']
  });

  get accountDestinoId() {
    return this.form.controls.accountDestinoId;
  }

  get valor() {
    return this.form.controls.valor;
  }

  onSubmit(): void {
    if (this.form.invalid) {
      this.form.markAllAsTouched();
      return;
    }

    const accountOrigemId = this.authService.getAccountId();
    if (!accountOrigemId) {
      this.errorMessage = 'Sessão inválida. Faça login novamente.';
      return;
    }

    this.errorMessage = null;
    this.isSubmitting = true;

    const { accountDestinoId, valor } = this.form.getRawValue();

    this.transferService.createTransfer({
      accountOrigemId,
      accountDestinoId: accountDestinoId!.trim(),
      valor: valor!
    }).subscribe({
      next: response => {
        this.isSubmitting = false;
        this.router.navigate(['/transferencia', response.transferId, 'status']);
      },
      error: (error: HttpErrorResponse) => {
        this.isSubmitting = false;
        this.errorMessage = this.mensagemDeErro(error);
      }
    });
  }

  private mensagemDeErro(error: HttpErrorResponse): string {
    if (error.status === 400) {
      return 'Dados inválidos. Verifique a conta de destino e o valor informados.';
    }
    if (error.status === 403) {
      return 'Você não tem permissão para transferir a partir desta conta.';
    }
    return 'Não foi possível iniciar a transferência. Tente novamente em instantes.';
  }
}
