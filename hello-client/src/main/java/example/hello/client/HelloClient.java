package example.hello.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.metadata.CompositeMetadataFlyweight;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Client that requests a hello message from the hello-service and prints the response.
 */
public class HelloClient {
    private static final Logger LOG = LoggerFactory.getLogger(HelloClient.class);

    public static void main(String... args) {
        final String name = getNameFromArgs(args);

        RSocket rSocket = RSocketFactory.connect()
                .transport(TcpClientTransport.create(7000))
                .start()
                .block();

        // Create the tracing identifiers to send in the metadata
        final String traceId = traceId();
        final String spanId = spanId();

        // Create the composite metadata
        final CompositeByteBuf compositeMetadata = buildCompositeMetadata(traceId, spanId);

        // Create the data buffer
        final ByteBuf data = ByteBufAllocator.DEFAULT.buffer().writeBytes(name.getBytes());

        LOG.info("Sending request for '{}' [traceId: '{}', spanId: '{}']", name, traceId, spanId);

        // Sending the request
        String response = rSocket.requestResponse(DefaultPayload.create(data, compositeMetadata))
                .map(Payload::getDataUtf8)
                .block();

        LOG.info("Response: {}", response);
    }

    /**
     * Builds a composite metadata buffer containing the traceId and spanId.
     *
     * @param traceId trace identifier
     * @param spanId span identifier
     * @return a {@link CompositeByteBuf}
     */
    private static CompositeByteBuf buildCompositeMetadata(final String traceId, final String spanId) {
        CompositeByteBuf metadataByteBuf = ByteBufAllocator.DEFAULT.compositeBuffer();

        // Adding the traceId to the composite metadata
        CompositeMetadataFlyweight.encodeAndAddMetadata(
                metadataByteBuf,
                ByteBufAllocator.DEFAULT,
                "messaging/x.traceId",
                ByteBufAllocator.DEFAULT.buffer().writeBytes(traceId.getBytes()));

        // Adding the spanId to the composite metadata
        CompositeMetadataFlyweight.encodeAndAddMetadata(
                metadataByteBuf,
                ByteBufAllocator.DEFAULT,
                "messaging/x.spanId",
                ByteBufAllocator.DEFAULT.buffer().writeBytes(spanId.getBytes()));

        return metadataByteBuf;
    }

    /**
     * Gets the name of the hello recipient from the command line arguments.
     *
     * @param args command line arguments
     * @return name of hello recipient
     */
    private static String getNameFromArgs(String... args) {
        if (args.length != 1) {
            throw new IllegalArgumentException("parameter 0 must be the name of the hello message recipient");
        }

        return args[0];
    }

    /**
     * Generates a random traceId.
     *
     * @return trace identifier
     */
    private static String traceId() {
        Random rand = new Random(System.currentTimeMillis());

        byte[] bytes = new byte[16];
        rand.nextBytes(bytes);

        return Hex.encodeHexString(bytes).toLowerCase();
    }

    /**
     * Generates a random spanId.
     *
     * @return span identifier
     */
    private static String spanId() {
        Random rand = new Random(System.currentTimeMillis());

        byte[] bytes = new byte[8];
        rand.nextBytes(bytes);

        return Hex.encodeHexString(bytes).toLowerCase();
    }
}
