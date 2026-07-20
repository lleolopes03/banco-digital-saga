export interface LoginRequest {
  email: string;
  senha: string;
}

export interface LoginResponse {
  token: string;
  accountId: string;
  expiresInSeconds: number;
}
