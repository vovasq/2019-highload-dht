package ru.mail.polis.dao;

import java.io.IOException;

class CustomDaoException extends IOException {
    private static final long serialVersionUID = -4296241150950572345L;

    CustomDaoException(final String message, final Throwable throwable) {
        super(message, throwable);
    }
}
