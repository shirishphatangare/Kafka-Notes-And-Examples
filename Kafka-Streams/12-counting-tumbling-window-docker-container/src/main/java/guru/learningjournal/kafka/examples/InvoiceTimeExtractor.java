package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.types.SimpleInvoice;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;

public class InvoiceTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long prevTime) {
        // 1) Get Invoice
        SimpleInvoice invoice = (SimpleInvoice) consumerRecord.value();
        // 2) Extract Created Time from the invoice and covert it to Epoch Millis
        long eventTime = Instant.parse(invoice.getCreatedTime()).toEpochMilli();
        // 3) Return valid non-negative time
        return ((eventTime>0) ? eventTime : prevTime);
    }
}
