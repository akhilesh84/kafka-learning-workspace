package com.learnkafka.common.serde;

import com.learnkafka.common.model.MarketPriceEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class JsonSerdeTest {

    @Test
    void testSerializationRoundtrip() {
        JsonSerializer<MarketPriceEvent> serializer = new JsonSerializer<>();
        JsonDeserializer<MarketPriceEvent> deserializer = new JsonDeserializer<>(MarketPriceEvent.class);

        MarketPriceEvent original = new MarketPriceEvent(
            "test-id-123",
            "BTC-USD",
            new BigDecimal("81546.89"),
            "USD",
            Instant.now(),
            "Coinbase Public API"
        );

        byte[] serialized = serializer.serialize("market-prices", original);
        assertNotNull(serialized);
        assertTrue(serialized.length > 0);

        MarketPriceEvent deserialized = deserializer.deserialize("market-prices", serialized);
        assertNotNull(deserialized);
        assertEquals(original.eventId(), deserialized.eventId());
        assertEquals(original.symbol(), deserialized.symbol());
        assertEquals(original.price(), deserialized.price());
        assertEquals(original.currency(), deserialized.currency());
        assertEquals(original.source(), deserialized.source());
    }
}
