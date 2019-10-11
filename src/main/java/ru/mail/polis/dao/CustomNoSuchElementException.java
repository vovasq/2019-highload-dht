package ru.mail.polis.dao;

import java.util.NoSuchElementException;

public class CustomNoSuchElementException extends NoSuchElementException {

    CustomNoSuchElementException(final String s) {
        super(s);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
