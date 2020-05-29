package ru.mail.polis.service.vovasq;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class BasicTopology implements Topology<String> {

    @NotNull
    private final String me;
    @NotNull
    private final String[] nodes;

    public BasicTopology(@NotNull final Set<String> nodes, @NotNull final String me) {
        this.me = me;
        this.nodes = new String[nodes.size()];
        nodes.toArray(this.nodes);
        Arrays.sort(nodes.toArray());
    }

    @Override
    public boolean isMe(@NotNull String node) {
        return node.equals(me);
    }

    @NotNull
    @Override
    public String primaryFor(@NotNull ByteBuffer key) {
        final int hash = key.hashCode();
        final int node = (hash & Integer.MAX_VALUE) % nodes.length;
        return nodes[node];
    }

    @NotNull
    @Override
    public List<String> getAll() {
        return List.of(nodes);
    }
}
