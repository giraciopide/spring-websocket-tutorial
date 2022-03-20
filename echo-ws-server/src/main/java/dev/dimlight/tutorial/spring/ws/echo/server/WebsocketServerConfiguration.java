package dev.dimlight.tutorial.spring.ws.echo.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebsocketServerConfiguration implements WebSocketConfigurer {

    private static final Logger log = LoggerFactory.getLogger(WebsocketServerConfiguration.class);
    private final EchoWebsocketHandler echoWebsocketHandler;

    public WebsocketServerConfiguration(EchoWebsocketHandler echoWebsocketHandler) {
        this.echoWebsocketHandler = echoWebsocketHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(echoWebsocketHandler, "echo");
        log.info("added echoWebsocketHandler to the WebSocketHandlerRegistry");
    }
}
