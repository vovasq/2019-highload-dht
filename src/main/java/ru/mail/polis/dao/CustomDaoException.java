package ru.mail.polis.dao;

import java.io.IOException;

class CustomDaoException extends IOException {
    CustomDaoException(String message, Throwable throwable)
    {
        super(message,throwable);
    }
}
