package com.learnkafka.common.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper;
    private Class<T> targetType;

    public JsonDeserializer() {
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public JsonDeserializer(Class<T> targetType) {
        this();
        this.targetType = targetType;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (targetType == null) {
            String className = (String) configs.get("json.deserializer.type");
            if (className != null) {
                try {
                    this.targetType = (Class<T>) Class.forName(className);
                } catch (ClassNotFoundException e) {
                    throw new SerializationException("Failed to find class: " + className, e);
                }
            }
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            if (targetType == null) {
                throw new SerializationException("Target type for JsonDeserializer not configured for topic: " + topic);
            }
            return objectMapper.readValue(data, targetType);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing JSON message for topic " + topic, e);
        }
    }
}
