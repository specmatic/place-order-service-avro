package com.example.order

import order.OrderRequest
import order.OrderStatus
import order.OrderToProcess
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Headers
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.nio.charset.StandardCharsets

@Service
class PlaceOrderService(
    private val kafkaTemplate: KafkaTemplate<String, OrderToProcess>,
) {
    companion object {
        private const val NEW_ORDERS_TOPIC = "new-orders"
        private const val WIP_ORDERS_TOPIC = "wip-orders"
        private const val CORRELATION_ID = "orderRequestId"
    }

    private val serviceName = this::class.simpleName

    init {
        println("$serviceName started running..")
    }

    @KafkaListener(topics = [NEW_ORDERS_TOPIC])
    fun placeOrder(record: ConsumerRecord<String, OrderRequest>) {
        val orderRequest = record.value()
        println("[$serviceName] Received message on topic $NEW_ORDERS_TOPIC - $orderRequest")

        val orderToProcess = OrderToProcess.newBuilder()
            .setId(orderRequest.id)
            .setStatus(OrderStatus.PROCESSING)
            .build()

        val correlationId = extractCorrelationId(record.headers())

        val message = ProducerRecord<String, OrderToProcess>(
            WIP_ORDERS_TOPIC,
            orderToProcess
        ).also {
            it.headers().add(CORRELATION_ID, correlationId.toString().toByteArray())
        }

        kafkaTemplate.send(message)
        println("[$serviceName] Sent message to topic $WIP_ORDERS_TOPIC - $orderToProcess")
    }

    private fun extractCorrelationId(headers: Headers): Int? {
        return headers.lastHeader(CORRELATION_ID)?.let {
            String(it.value(), StandardCharsets.UTF_8).toInt()
        }
    }
}