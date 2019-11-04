package ru.mail.polis.service.vovasq;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class for processing requests in cluster.
 */
class ReplicasManager {

    @NotNull
    private final ReplicasParams params;
    @NotNull
    private final Topology<String> topology;
    @NotNull
    private final ByteBuffer key;


    ReplicasManager(@NotNull ReplicasParams params,
                    @NotNull Topology<String> topology,
                    @NotNull ByteBuffer key) {
        this.params = params;
        this.topology = topology;
        this.key = key;
    }

    List<String> getNodesToCall() {
        final List<String> sortedNodes = topology
                .all()
                .stream()
                .sorted()
                .collect(Collectors.toList());
        final int firstNodeIndex = sortedNodes.indexOf(topology.primaryFor(key));
        if(sortedNodes.size() < params.getFrom()){
            throw new IllegalArgumentException("We got only less nodes than ou want!!");
        }
        return sortedNodes.subList(firstNodeIndex,firstNodeIndex+params.getFrom());
    }

    void processInternalRequest() {

    }


}
