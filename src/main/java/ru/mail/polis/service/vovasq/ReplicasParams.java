package ru.mail.polis.service.vovasq;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ReplicasParams {

    private final int ack;
    private final int from;

    private ReplicasParams(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }

    @NotNull
    static ReplicasParams of(@Nullable final String replicas, final int clusterSize) throws IllegalArgumentException {
        if (replicas != null && replicas.contains("/")) {
            final int ack = Integer.parseInt(Iterables.get(Splitter.on('/').split(replicas), 0));
            final int from = Integer.parseInt(Iterables.get(Splitter.on('/').split(replicas), 1));
            if(ack > from){
                throw new IllegalArgumentException("ack is more than from");
            }
            return new ReplicasParams(ack, from);
        }

        if (clusterSize == 1) {
            new ReplicasParams(1, 1);
        }

        return new ReplicasParams(clusterSize / 2 + 1, clusterSize);
    }

}
