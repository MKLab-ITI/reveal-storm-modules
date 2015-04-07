package io.latent.storm.rabbitmq;

import com.rabbitmq.client.QueueingConsumer;

import java.util.HashMap;
import java.util.Map;

public class Message {
    public static final Message NONE = new None();

    private final byte[] body;

    public Message(byte[] body) {
        this.body = body;
    }

    public static Message forDelivery(QueueingConsumer.Delivery delivery) {
        return (delivery != null) ? new DeliveredMessage(delivery) : NONE;
    }

    public static Message forSending(byte[] body) {
        return (body != null) ? new Message(body) : NONE;
    }

    public static Message forSending(byte[] body,
                                     Map<String, Object> headers,
                                     String exchangeName,
                                     String routingKey,
                                     String contentType,
                                     String contentEncoding,
                                     boolean persistent) {
        return (body != null && exchangeName != null && exchangeName.length() > 0) ?
                new MessageForSending(body, headers, exchangeName, routingKey, contentType, contentEncoding, persistent) :
                NONE;
    }

    public byte[] getBody() {
        return body;
    }


    public static class DeliveredMessage extends Message {
        private final boolean redelivery;
        private final long deliveryTag;
        private final String receivedRoutingKey;
        private final String receivedExchange;

        private DeliveredMessage(QueueingConsumer.Delivery delivery) {
            super(delivery.getBody());
            redelivery = delivery.getEnvelope().isRedeliver();
            deliveryTag = delivery.getEnvelope().getDeliveryTag();
            receivedRoutingKey = delivery.getEnvelope().getRoutingKey();
            receivedExchange = delivery.getEnvelope().getExchange();
        }

        public boolean isRedelivery() {
            return redelivery;
        }

        public long getDeliveryTag() {
            return deliveryTag;
        }

        public String getReceivedRoutingKey() {
            return receivedRoutingKey;
        }

        public String getReceivedExchange() {
            return receivedExchange;
        }
    }

    public static class None extends Message {
        private None() {
            super(null);
        }

        @Override
        public byte[] getBody() {
            throw new UnsupportedOperationException();
        }

        ;
    }

    public static class MessageForSending extends Message {
        private final Map<String, Object> headers;
        private final String exchangeName;
        private final String routingKey;
        private final String contentType;
        private final String contentEncoding;
        private final boolean persistent;

        private MessageForSending(byte[] body,
                                  Map<String, Object> headers,
                                  String exchangeName,
                                  String routingKey,
                                  String contentType,
                                  String contentEncoding,
                                  boolean persistent) {
            super(body);
            this.headers = (headers != null) ? headers : new HashMap<String, Object>();
            this.exchangeName = exchangeName;
            this.routingKey = routingKey;
            this.contentType = contentType;
            this.contentEncoding = contentEncoding;
            this.persistent = persistent;
        }

        public Map<String, Object> getHeaders() {
            return headers;
        }

        public String getExchangeName() {
            return exchangeName;
        }

        public String getRoutingKey() {
            return routingKey;
        }

        public String getContentType() {
            return contentType;
        }

        public String getContentEncoding() {
            return contentEncoding;
        }

        public boolean isPersistent() {
            return persistent;
        }
    }
}
