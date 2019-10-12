package ru.mail.polis.dao;

import java.util.NoSuchElementException;

public class CustomNoSuchElementException extends NoSuchElementException {

    CustomNoSuchElementException(final String s) {
        super(s);
    }

    @Override
    public Throwable fillInStackTrace() {
        synchronized (this){
            return this;
        }
    }
}
