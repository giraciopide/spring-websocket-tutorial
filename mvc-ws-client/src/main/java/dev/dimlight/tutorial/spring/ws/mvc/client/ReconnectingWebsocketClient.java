package dev.dimlight.tutorial.spring.ws.mvc.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.util.unit.DataSize;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import javax.websocket.ContainerProvider;
import javax.websocket.WebSocketContainer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A websocket client that will always try to reconnect until explicitly shutdown.
 * <p>
 * Why this exists? What does this wrapper provides?
 * <li>threadsafe</li>
 * <li>automatic reconnection </li>
 * <li>non blocking sends</li>
 * <li>a connection status listener, which need to be set at creation time, so that you can't skip events, you need
 * to decide what to do with the connection status before you instantiate this</li>.
 *
 * @author Marco Nicolini
 */
public class ReconnectingWebsocketClient {

    private static final Logger log = LoggerFactory.getLogger(ReconnectingWebsocketClient.class);

    private static final ThreadFactory daemonsThreadFactory = task -> {
        final Thread thread = new Thread(task);
        thread.setDaemon(true);
        return thread;
    };

    private final AtomicLong connectionAttemptCount = new AtomicLong(0);
    private final ScheduledExecutorService retryReconnectScheduler;
    private final ExecutorService sessionExecutor;
    private final URI uri;
    private final WebSocketHttpHeaders headers;

    private final Consumer<ConnectionStatus> connectionStatusListener;
    private final Consumer<TextMessage> messageConsumer;
    private final Duration sendTimeout;
    private final DataSize maxOutputBuffer;
    private final ReconnectBehavior reconnectBehavior;

    // set to the current session, gets overwritten every time a new connection is made (only when the previous
    // one has failed at the transport layer).
    private volatile CompletableFuture<ConcurrentWebSocketSessionDecorator> sessionFut;

    // main constructor
    public ReconnectingWebsocketClient(
        URI uri,
        WebSocketHttpHeaders headers,
        Duration sendTimeout,
        DataSize maxOutputBuffer,
        Consumer<ConnectionStatus> connectionStatusListener,
        Consumer<TextMessage> messageConsumer,
        ReconnectBehavior reconnectBehavior) {
        this.uri = uri;
        this.headers = headers;
        this.sendTimeout = sendTimeout;
        this.maxOutputBuffer = maxOutputBuffer;
        this.retryReconnectScheduler = Executors.newSingleThreadScheduledExecutor(daemonsThreadFactory);
        this.sessionExecutor = Executors.newSingleThreadExecutor(daemonsThreadFactory);
        this.connectionStatusListener = (connectionStatusListener != null) ? connectionStatusListener : doNothingConsumer();
        this.messageConsumer = (messageConsumer != null) ? messageConsumer : doNothingConsumer();
        this.reconnectBehavior = reconnectBehavior;
    }

    /**
     * Starts the connection.
     *
     * @return a future that is completed when the first connection attempt completes (either successfully or with a failure).
     */
    public CompletableFuture<Void> start() {
        reconnectBehavior.setup(new ReconnectBehaviorContext() {
            @Override
            public CompletableFuture<ConcurrentWebSocketSessionDecorator> reconnect() {
                return ReconnectingWebsocketClient.this.connect();
            }

            @Override
            public ScheduledExecutorService scheduler() {
                return retryReconnectScheduler;
            }

            @Override
            public void setCurrentConnection(CompletableFuture<ConcurrentWebSocketSessionDecorator> sessionFuture) {
                ReconnectingWebsocketClient.this.sessionFut = sessionFuture;
            }
        });
        notifyConnectionStatus(ConnectionStatus.STARTING);
        final CompletableFuture<ConcurrentWebSocketSessionDecorator> firstConnectionAttempt = connect();
        this.sessionFut = firstConnectionAttempt;

        // note that we don't return the field, but the local variable. The field could have been set already
        // by the reconnect behavior after the first call to connect()
        return firstConnectionAttempt.thenApply(session -> null);
    }

    /**
     * Stops the connection/reconnection cycle and closes down the internal executors of the client.
     *
     * @return a future that is completed when the client is fully shutdown, including its internal executors.
     */
    public CompletableFuture<Void> shutdown(Duration graceTime) {
        // we execute this in a new thread because we can't wait for an execution shutdown from within the
        // executor itself, as it would be deadlocking.
        return CompletableFuture.supplyAsync(() -> {
            joinAndLogExceptionsIfAny(close(CloseStatus.GOING_AWAY), "while closing websocket session at client shutdown");
            retryReconnectScheduler.shutdownNow();
            sessionExecutor.shutdown();
            try {
                final boolean terminatedInTime = sessionExecutor.awaitTermination(graceTime.toMillis(), TimeUnit.MILLISECONDS);
                if (!terminatedInTime) {
                    log.warn("Session executor did not terminate within its shutdown timeout [{}] shutting down forcefully", graceTime);
                    final List<Runnable> tasksStillToRun = sessionExecutor.shutdownNow();
                    log.warn("Session executor was shutdown with [{}] tasks still in queue", tasksStillToRun.size());
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted while awaiting termination of the session executor [{}]", e.getMessage(), e);
                Thread.currentThread().interrupt(); // do not swallow InterruptedException ever
            }

            return null;
        }, naiveNewThreadExecutor());
    }

    private CompletableFuture<ConcurrentWebSocketSessionDecorator> connect() {
        final WebSocketContainer webSocketContainer = ContainerProvider.getWebSocketContainer();
        final StandardWebSocketClient standardWebSocketClient = new StandardWebSocketClient(webSocketContainer);

        standardWebSocketClient.setTaskExecutor(new ConcurrentTaskExecutor(sessionExecutor));

        log.info("websocket connection attempt #[{}]", connectionAttemptCount.incrementAndGet());
        final TextWebSocketHandler handler = new TextWebSocketHandler() {
            @Override
            public void afterConnectionEstablished(WebSocketSession session) {
                log.info("connection #[{}] opened [{}]", connectionAttemptCount.get(), session);
                notifyConnectionStatus(ReconnectingWebsocketClient.ConnectionStatus.UP);
            }

            @Override
            public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
                notifyConnectionStatus(ReconnectingWebsocketClient.ConnectionStatus.DOWN);
                log.warn("connection closed [{}] status [{}], will retry in [{}]]", session, status, reconnectBehavior.intervalToNextReconnection());
                reconnectBehavior.afterConnectionClosed();
            }

            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) {
                log.info("RECV msg [{}]", message);
                notifyMessage(message);
            }

            @Override
            public void handleTransportError(WebSocketSession session, Throwable exception) {
                log.error("Transport error!");
            }
        };

        return standardWebSocketClient.doHandshake(handler, headers, uri)
            .completable()
            .thenApply(this::wrapInConcurrentDecorator)
            .whenComplete((session, ex) -> {
                if (ex != null) {
                    log.warn("connection attempt #[{}] failed, will retry in [{}] [{}]", connectionAttemptCount.get(), reconnectBehavior.intervalToNextReconnection(), ex.getMessage(), ex);
                    reconnectBehavior.afterConnectionAttemptFailed();
                }
            });
    }

    private static <T> Consumer<T> doNothingConsumer() {
        return (x) -> {
        };
    }

    private void notifyConnectionStatus(ConnectionStatus connectionStatus) {
        try {
            log.trace("notifying connection status [{}]", connectionStatus);
            this.connectionStatusListener.accept(connectionStatus);
        } catch (Exception ex) {
            log.warn("Exception while executing connection listener on websocket to [{}] msg [{}]", uri, ex.getMessage(), ex);
        }
    }

    private void notifyMessage(TextMessage message) {
        try {
            this.messageConsumer.accept(message);
        } catch (Exception ex) {
            log.warn("Exception while executing connection listener on websocket to [{}] msg [{}]", uri, ex.getMessage(), ex);
        }
    }

    private ConcurrentWebSocketSessionDecorator wrapInConcurrentDecorator(WebSocketSession session) {
        return new ConcurrentWebSocketSessionDecorator(
            session,
            Math.toIntExact(sendTimeout.toMillis()),
            Math.toIntExact(maxOutputBuffer.toBytes()),
            ConcurrentWebSocketSessionDecorator.OverflowStrategy.TERMINATE);
    }

    /**
     * Non blocking send.
     *
     * @return a {@link CompletableFuture} that completes when the sending is done.
     */
    public CompletableFuture<Void> send(String message) {
        return sessionFut
            .thenCompose(this::failIfNotOpen)
            .thenAcceptAsync(session -> sendBlocking(message, session), sessionExecutor);
    }

    private CompletableFuture<ConcurrentWebSocketSessionDecorator> failIfNotOpen(ConcurrentWebSocketSessionDecorator session) {
        if (session.isOpen()) {
            return CompletableFuture.completedFuture(session);
        } else {
            return CompletableFuture.failedFuture(new IOException("Websocket client is not connected to [" + uri + "]"));
        }
    }

    private static void sendBlocking(String message, ConcurrentWebSocketSessionDecorator session) {
        try {
            session.sendMessage(new TextMessage(message));
            log.info("SENT msg [{}]", message);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Closes the connection with the given reason, the connection will be reattempted after the reconnection interval
     * has passed. To completely shut down, use the shutdown method.
     *
     * @param closeStatus the reason why you're closing the connection.
     * @return a completable future that completes when the underlying websocket session closes.
     */
    public CompletableFuture<Void> close(CloseStatus closeStatus) {
        return sessionFut.thenAcceptAsync(session -> {
            if (session.isOpen()) {
                log.info("performing blocking close on session [{}]", session.getId());
                closeBlocking(closeStatus, session);
                log.info("session [{}] was closed", session.getId());
            } else {
                log.info("Skipping session close since session [{}] is not open", session.getId());
            }
        }, sessionExecutor);
    }

    private static void closeBlocking(CloseStatus closeStatus, ConcurrentWebSocketSessionDecorator session) {
        try {
            session.close(closeStatus);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new UncheckedIOException(e);
        }
    }

    /**
     * @return a new executor that starts a new thread for each submitted task: pro of this naive implementation, you
     * don't have to close it, ever.
     */
    private static Executor naiveNewThreadExecutor() {
        return new Executor() {
            private final AtomicLong id = new AtomicLong(0);

            @Override
            public void execute(Runnable command) {
                final Thread thread = new Thread(command);
                thread.setDaemon(true);
                thread.setName("throwaway-" + id.incrementAndGet());
                thread.setUncaughtExceptionHandler((t, e) -> log.warn("Uncaught execption in thread [{}] message [{}]", t.getName(), e.getMessage(), e));
                thread.start();
            }
        };
    }

    /**
     * Joins a future, throwing away any result and logging any exceptions.
     *
     * @param errorMessagePrefix a description of what you're expecting while joining the future so that exceptions
     *                           can be prefixed by it to not lose context within logs.
     */
    private static void joinAndLogExceptionsIfAny(CompletableFuture<?> future, String errorMessagePrefix) {
        try {
            future.join();
        } catch (RuntimeException ex) {
            log.warn(errorMessagePrefix + " message [{}]", ex.getMessage(), ex);
        }
    }

    public enum ConnectionStatus {
        /**
         * Not yet tried to connect.
         */
        STARTING,

        /**
         * Already tried to connect at least once and currently down.
         */
        DOWN,

        /**
         * Connected.
         */
        UP
    }

    /**
     *
     */
    interface ReconnectBehaviorContext {
        CompletableFuture<ConcurrentWebSocketSessionDecorator> reconnect();

        ScheduledExecutorService scheduler();

        void setCurrentConnection(CompletableFuture<ConcurrentWebSocketSessionDecorator> sessionFuture);
    }

    /**
     *
     */
    public interface ReconnectBehavior {

        Duration intervalToNextReconnection();

        void setup(ReconnectBehaviorContext ctx);

        void afterConnectionAttemptFailed();

        void afterConnectionClosed();
    }

    /**
     * Courtesy static factory for common {@link ReconnectBehavior}.
     */
    public interface ReconnectBehaviors {
        static ReconnectBehavior sendFailsFast(Duration reconnectionInterval) {
            return new SendsWillFailFastReconnectBehavior(reconnectionInterval);
        }

        static ReconnectBehavior sendWaitNextConnectionAtttempt(Duration reconnectionInterval) {
            return new SendsWillWaitForNextReconnectionTryReconnectBehavior(reconnectionInterval);
        }
    }

    /**
     * A {@link ReconnectBehavior} with fixed time intervals that sets the current connection future only when
     * the connection attempt has been done (failed or successfully).
     * This has the effect that trying to use the connection while disconnected and a reconnection is scheduled
     * all call to sends will fail, because the connection is closed.
     */
    public static class SendsWillFailFastReconnectBehavior implements ReconnectBehavior {

        private ReconnectBehaviorContext ctx;
        private final Duration fixedReconnectionInterval;

        public SendsWillFailFastReconnectBehavior(Duration fixedReconnectionInterval) {
            this.fixedReconnectionInterval = fixedReconnectionInterval;
        }

        @Override
        public Duration intervalToNextReconnection() {
            return fixedReconnectionInterval;
        }

        @Override
        public void setup(ReconnectBehaviorContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void afterConnectionAttemptFailed() {
            scheduleNextAttempt();
        }

        @Override
        public void afterConnectionClosed() {
            scheduleNextAttempt();
        }

        private void scheduleNextAttempt() {
            ctx.scheduler().schedule(() -> ctx.setCurrentConnection(ctx.reconnect()), fixedReconnectionInterval.toMillis(), TimeUnit.MILLISECONDS);
            log.info("scheduled reconnect in [{}]", fixedReconnectionInterval);
        }
    }

    /**
     * A {@link ReconnectBehavior} with fixed time intervals that set the current connection future as soon as the
     * next connection attempt has been scheduled.
     * This has the effect that trying to use the connection while disconnected (with a pending reconnection schedule)
     * all calls to send will wait on the next connection attempt (and then fail or succeed depending on the result
     * of the connection attempt).
     */
    public static class SendsWillWaitForNextReconnectionTryReconnectBehavior implements ReconnectBehavior {

        private final Duration fixedReconnectionInterval;
        private ReconnectBehaviorContext ctx;

        public SendsWillWaitForNextReconnectionTryReconnectBehavior(Duration fixedReconnectionInterval) {
            this.fixedReconnectionInterval = fixedReconnectionInterval;
        }

        @Override
        public Duration intervalToNextReconnection() {
            return fixedReconnectionInterval;
        }

        @Override
        public void setup(ReconnectBehaviorContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void afterConnectionAttemptFailed() {
            scheduleRetryAndSetConnectionToNextAttempt();
        }

        @Override
        public void afterConnectionClosed() {
            scheduleRetryAndSetConnectionToNextAttempt();
        }

        private void scheduleRetryAndSetConnectionToNextAttempt() {
            final CompletableFuture<ConcurrentWebSocketSessionDecorator> nextAttemptConnection = flatScheduleOnce(ctx.scheduler(), ctx::reconnect, fixedReconnectionInterval);
            ctx.setCurrentConnection(nextAttemptConnection);
            log.info("scheduled reconnect in [{}]", fixedReconnectionInterval);
        }

        /**
         * @param futureValueSupplier something that produces the future of a result.
         * @param delay               the time after which the futureSupplier should be called.
         * @param <T>                 the type of result.
         * @return a {@link CompletableFuture} that completes when the {@link CompletableFuture} produced by the futureValueSupplier
         * completes.
         */
        private static <T> CompletableFuture<T> flatScheduleOnce(ScheduledExecutorService scheduler, Supplier<CompletableFuture<T>> futureValueSupplier, Duration delay) {
            final CompletableFuture<CompletableFuture<T>> out = new CompletableFuture<>();
            scheduler.schedule(
                () -> {
                    try {
                        out.complete(futureValueSupplier.get());
                    } catch (RuntimeException ex) {
                        out.completeExceptionally(ex);
                    }
                },
                delay.toMillis(),
                TimeUnit.MILLISECONDS
            );
            return out.thenComposeAsync(Function.identity());
        }
    }
}
