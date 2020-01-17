package example.hello.service;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.SocketAcceptor;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.metadata.CompositeMetadata;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Service that returns hello messages requested by the hello-client.
 */
public class HelloService {
    private static final Logger LOG = LoggerFactory.getLogger(HelloService.class);
    private static final String MESSAGE_FORMAT = "Hello, %s! [traceId: '%s', spanId: '%s']";

    public static void main(String... args) throws Exception {
        RSocketFactory.receive()
                .frameDecoder(PayloadDecoder.DEFAULT)
                .acceptor(new SocketAcceptor() {
                    @Override
                    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
                        return Mono.just(new AbstractRSocket() {
                            @Override
                            public Mono<Payload> requestResponse(Payload payload) {
                                final String name = payload.getDataUtf8();
                                final Map<String, Object> metadata = parseMetadata(payload);

                                final String response = String.format(MESSAGE_FORMAT,
                                        name,
                                        metadata.getOrDefault("messaging/x.traceId", ""),
                                        metadata.getOrDefault("messaging/x.spanId", ""));

                                LOG.info("Sending message: {}", response);

                                return Mono.just(DefaultPayload.create(response.getBytes()));
                            }
                        });
                    }
                })
                .transport(TcpServerTransport.create(7000))
                .start()
                .block();

        LOG.info("RSocket server started on port: 7000");

        Thread.currentThread().join();
    }

    /**
     * Parses the incoming composite metadata.
     *
     * @param payload incoming payload
     * @return a map containing the composite metadata entries
     */
    private static Map<String, Object> parseMetadata(Payload payload) {
        Map<String, Object> metadataMap = new HashMap<>();

        CompositeMetadata compositeMetadata = new CompositeMetadata(payload.metadata(), true);
        compositeMetadata.forEach(entry -> {
            byte[] bytes = new byte[entry.getContent().readableBytes()];
            entry.getContent().readBytes(bytes);

            metadataMap.put(entry.getMimeType(), new String(bytes, StandardCharsets.UTF_8));
        });

        return metadataMap;
    }
}

