package ru.mail.polis.dao;

import java.io.IOException;

class CustomDaoException extends IOException {
    CustomDaoException(final String message, final Throwable throwable)
    {
        super(message,throwable);
    }
}
