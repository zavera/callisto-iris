package tech.callisto.iris.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.callisto.iris.routing.DeadLetterChannel;
import tech.callisto.iris.routing.IrisHandler;
import tech.callisto.iris.transport.IrisTransport;

import java.util.HashMap;
import java.util.Map;

public class IrisPipeline {

    private static final Logger log = LoggerFactory.getLogger(IrisPipeline.class);

    private final IrisTransport transport;
    private final Map<String, IrisHandler> routes = new HashMap<>();
    private final DeadLetterChannel deadLetter = new DeadLetterChannel();
    private int maxRetries = 3;

    private IrisPipeline(Builder builder) {
        this.transport = builder.transport;
        this.routes.putAll(builder.routes);
        this.maxRetries = builder.maxRetries;
    }

    public void start() {
        transport.connect();
        routes.forEach((channel, handler) ->
            transport.subscribe(channel, (ch, payload) -> dispatch(ch, payload, handler, 0))
        );
        log.info("Iris pipeline started — listening on {} channel(s)", routes.size());
    }

    private void dispatch(String channel, String payload, IrisHandler handler, int attempt) {
        try {
            handler.handle(channel, payload);
        } catch (Exception e) {
            if (attempt < maxRetries) {
                log.warn("Handler failed on channel {} (attempt {}/{}), retrying", channel, attempt + 1, maxRetries);
                dispatch(channel, payload, handler, attempt + 1);
            } else {
                deadLetter.record(channel, payload, e);
            }
        }
    }

    public void stop() {
        transport.disconnect();
        log.info("Iris pipeline stopped.");
    }

    public DeadLetterChannel getDeadLetterChannel() {
        return deadLetter;
    }

    public static Builder builder(IrisTransport transport) {
        return new Builder(transport);
    }

    public static class Builder {
        private final IrisTransport transport;
        private final Map<String, IrisHandler> routes = new HashMap<>();
        private int maxRetries = 3;

        private Builder(IrisTransport transport) {
            this.transport = transport;
        }

        public Builder listen(String channel, IrisHandler handler) {
            routes.put(channel, handler);
            return this;
        }

        public Builder maxRetries(int retries) {
            this.maxRetries = retries;
            return this;
        }

        public IrisPipeline build() {
            return new IrisPipeline(this);
        }
    }
}
