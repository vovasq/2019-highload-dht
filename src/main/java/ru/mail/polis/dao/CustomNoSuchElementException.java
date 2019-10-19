package ru.mail.polis.dao;

import java.util.NoSuchElementException;

public class CustomNoSuchElementException extends NoSuchElementException {

    private static final long serialVersionUID = -2147812170273686770L;

    CustomNoSuchElementException(final String s) {
        super(s);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
            return this;
    }
}
