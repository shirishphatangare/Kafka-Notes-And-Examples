package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/* Here Custom Partitioner is used to ensure that all invoices from same customer comes to the same partition.
*  No matter from which store customer is buying, all invoices should land to the same partition.
*  Every Task will have it's own local state store and this way all invoices of a customer will land into the same partition and
*  hence to the same Task and will use same State Store. This way we will not loose any data for that customer.
*  If customers are split over different partitions and hence different Tasks, we will loose some data due to different state store per-Task.
*/

public class RewardsPartitioner implements StreamPartitioner<String, PosInvoice> {
    private static final Logger logger = LogManager.getLogger();
    @Override
    public Integer partition(String topic, String key, PosInvoice value, int numPartitions) {
        int i = Math.abs(value.getCustomerCardNo().hashCode()) % numPartitions;
        return i;
    }
}
