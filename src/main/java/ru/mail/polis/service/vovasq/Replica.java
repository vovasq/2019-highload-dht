package ru.mail.polis.service.vovasq;

import com.google.common.base.Splitter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;

public class Replica {
    final int ack;
    final int from;

    private static final Logger log = LogManager.getLogger("default");

    Replica(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    static Replica of(final String param, final Replica defaultRf) {
        if (param == null) {
            log.info("defaultRf was returned");
            return defaultRf;
        }
        final List<String> values = Splitter.on('/').splitToList(handleValue(param));
        if (!checkValues(values)) {
            throw new IllegalArgumentException("values not valid");
        }
        return new Replica(Integer.parseInt(values.get(0)), Integer.parseInt(values.get(1)));
    }

    private static String handleValue(final String value) {
        return value.replace("=", "");
    }

    private static Boolean checkValues(List<String> values) {
        final String firstValue = values.get(0);
        final String secondValue = values.get(1);

        return toInt(firstValue) >= 1 || toInt(secondValue) >= 1 || toInt(firstValue) <= toInt(secondValue);
    }

    private static int toInt(String line) {
        return Integer.parseInt(line);
    }


}
