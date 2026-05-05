package com.pinkline.kafkabridge.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * RabbitmqTopologyConfig
 *
 * Auto-declares durable queues + bindings on startup via Spring AMQP's RabbitAdmin.
 * Fresh RabbitMQ instances Just Work — no manual queue creation needed.
 *
 * Queues declared here:
 *   scada.monitor.queue     — optional in-bridge monitor feed (bridge.monitor.enabled)
 *   scada.tms.alarms.queue  — always-on inbound: SCADA RSAE → TMS Artemis
 */
@Configuration
public class RabbitmqTopologyConfig {

    private final BridgeConfig config;

    public RabbitmqTopologyConfig(BridgeConfig config) {
        this.config = config;
    }

    @Bean
    public TopicExchange scadaExchange() {
        // amq.topic is built-in to RabbitMQ. Declaring it idempotently is safe.
        return new TopicExchange("amq.topic", true, false);
    }

    // ── Monitor queue (optional) ─────────────────────────────────────

    @Bean
    public Queue monitorQueue() {
        if (!config.getMonitor().isEnabled()) {
            return new Queue("bridge.monitor.disabled", false, true, true);
        }
        return new Queue(config.getMonitor().getQueue(), true, false, false);
    }

    @Bean
    public Binding monitorBinding(Queue monitorQueue, TopicExchange scadaExchange) {
        if (!config.getMonitor().isEnabled()) {
            return BindingBuilder.bind(monitorQueue).to(scadaExchange).with("bridge.monitor.disabled");
        }
        return BindingBuilder.bind(monitorQueue)
                .to(scadaExchange)
                .with(config.getMonitor().getRoutingKey());
    }

    // ── Inbound queues: one per bridge.inbound[n] entry ─────────────

    @Bean
    public List<Queue> inboundQueues() {
        List<Queue> queues = new ArrayList<>();
        for (BridgeConfig.InboundRoute inb : config.getInbound()) {
            if (inb.getQueue() != null && !inb.getQueue().isBlank()) {
                queues.add(new Queue(inb.getQueue(), true, false, false));
            }
        }
        return queues;
    }

    @Bean
    public List<Binding> inboundBindings(List<Queue> inboundQueues, TopicExchange scadaExchange) {
        List<Binding> bindings = new ArrayList<>();
        List<BridgeConfig.InboundRoute> routes = config.getInbound();
        for (int i = 0; i < inboundQueues.size(); i++) {
            bindings.add(BindingBuilder.bind(inboundQueues.get(i))
                    .to(scadaExchange)
                    .with(routes.get(i).getRoutingKey()));
        }
        return bindings;
    }
}
