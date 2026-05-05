package com.pinkline.kafkabridge.api;

import org.springframework.stereotype.Component;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * In-memory store for decrypted SCADA messages received from RabbitMQ.
 * Holds last 100 messages. Notifies SSE subscribers on each new message.
 */
@Component
public class MessageStore {

    public record ScadaMessage(long id, String receivedAt, String topic, String json) {}

    private final CopyOnWriteArrayList<ScadaMessage> messages = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<ScadaMessage>> subscribers = new CopyOnWriteArrayList<>();
    private long counter = 0;

    public synchronized void add(String topic, String json) {
        ScadaMessage msg = new ScadaMessage(++counter, Instant.now().toString(), topic, json);
        messages.add(msg);
        if (messages.size() > 100) messages.remove(0);
        subscribers.forEach(s -> s.accept(msg));
    }

    public List<ScadaMessage> getAll() {
        return new ArrayList<>(messages);
    }

    public void subscribe(Consumer<ScadaMessage> listener) {
        subscribers.add(listener);
    }

    public void unsubscribe(Consumer<ScadaMessage> listener) {
        subscribers.remove(listener);
    }
}
