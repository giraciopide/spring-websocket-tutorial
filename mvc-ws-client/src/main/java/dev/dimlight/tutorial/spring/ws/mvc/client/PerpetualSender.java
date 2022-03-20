package dev.dimlight.tutorial.spring.ws.mvc.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.unit.DataSize;
import org.springframework.web.socket.WebSocketHttpHeaders;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class PerpetualSender {

    private static final Logger log = LoggerFactory.getLogger(PerpetualSender.class);
    private final ReconnectingWebsocketClient client;
    private final AtomicLong counter = new AtomicLong(0);

    public PerpetualSender() {
        this.client = new ReconnectingWebsocketClient(
            URI.create("ws://localhost:8080/echo"),
            new WebSocketHttpHeaders(),
            Duration.ofSeconds(20),
            DataSize.ofMegabytes(10),
            status -> log.info("received status [{}] status", status),
            message -> log.info("received messsage [{}]", message),
            ReconnectingWebsocketClient.ReconnectBehaviors.sendFailsFast(Duration.ofSeconds(3)));
    }

    @Scheduled(fixedDelayString = "PT10S")
    public void sendMessage() {
        final long msgNum = counter.incrementAndGet();
        log.info("periodic message trigger fired! (msg #{})", msgNum);
        client.send("*" + msgNum + "*").join();
    }

    @PostConstruct
    public void atStart() {
        client.start().join();
        log.info("client was started");
    }

    @PreDestroy
    public void shutdown() {
        client.shutdown(Duration.ofSeconds(10)).join();
        log.info("client was shutdown");
    }
}
