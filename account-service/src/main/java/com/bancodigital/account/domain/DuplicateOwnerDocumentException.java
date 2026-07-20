package com.bancodigital.account.domain;

public class DuplicateOwnerDocumentException extends RuntimeException {

    public DuplicateOwnerDocumentException(String ownerDocument) {
        super("Já existe uma conta para o documento: " + ownerDocument);
    }
}
