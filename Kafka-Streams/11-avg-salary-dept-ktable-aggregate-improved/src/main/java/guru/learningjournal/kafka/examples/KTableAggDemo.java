package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.DepartmentAggregate;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

/*
* This example is an improved version of 09-avg-salary-dept-kstream-aggregate-glitch
* You can observe differences in update scenario by executing and comparing resutls for 09-avg-salary-dept-kstream-aggregate-glitch and 11-avg-salary-dept-ktable-aggregate-improved
*/

public class KTableAggDemo {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreLocation);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,100);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.table(AppConfigs.topicName,
            Consumed.with(AppSerdes.String(), AppSerdes.Employee()))
             // For KTable groupBy returns a Key-Value Pair, whereas for KStream  groupBy returns a Key
            .groupBy((k, v) -> KeyValue.pair(v.getDepartment(), v), Grouped.with(AppSerdes.String(), AppSerdes.Employee()))
            .aggregate(
                //Initializer
                () -> new DepartmentAggregate()
                    .withEmployeeCount(0)
                    .withTotalSalary(0)
                    .withAvgSalary(0D),
                //Adder
                (k, v, aggV) -> new DepartmentAggregate()
                    .withEmployeeCount(aggV.getEmployeeCount() + 1)
                    .withTotalSalary(aggV.getTotalSalary() + v.getSalary())
                    .withAvgSalary((aggV.getTotalSalary() + v.getSalary()) / (aggV.getEmployeeCount() + 1D)),
                //Subtractor
                (k, v, aggV) -> new DepartmentAggregate()
                    .withEmployeeCount(aggV.getEmployeeCount() - 1)
                    .withTotalSalary(aggV.getTotalSalary() - v.getSalary())
                    .withAvgSalary((aggV.getTotalSalary() - v.getSalary()) / (aggV.getEmployeeCount() - 1D)),
                //Serializer
                Materialized.<String, DepartmentAggregate, KeyValueStore<Bytes, byte[]>>as(
                    AppConfigs.stateStoreName).withValueSerde(AppSerdes.DepartmentAggregate())
            ).toStream().print(Printed.<String, DepartmentAggregate>toSysOut().withLabel("Department Aggregate"));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            streams.close();
        }));
    }
}

/*

Give initial inputs line by line

101:{"id": "101", "name": "Prashant", "department": "engineering", "salary": 5000}
102:{"id": "102", "name": "John", "department": "accounts", "salary": 8000}
103:{"id": "103", "name": "Abdul", "department": "engineering", "salary": 3000}
104:{"id": "104", "name": "Melinda", "department": "support", "salary": 7000}
105:{"id": "105", "name": "Jimmy", "department": "support", "salary": 6000}

Then give below inputs line by line

101:{"id": "101", "name": "Prashant", "department": "support", "salary": 5000}
104:{"id": "104", "name": "Melinda", "department": "engineering", "salary": 7000}

Observe output - Updates happening properly since we are using KTable

[Department Aggregate]: engineering, guru.learningjournal.kafka.examples.types.DepartmentAggregate@54b6c3d8[totalSalary=5000,employeeCount=1,avgSalary=5000.0]
[Department Aggregate]: accounts, guru.learningjournal.kafka.examples.types.DepartmentAggregate@728e587c[totalSalary=8000,employeeCount=1,avgSalary=8000.0]
[Department Aggregate]: engineering, guru.learningjournal.kafka.examples.types.DepartmentAggregate@16433352[totalSalary=8000,employeeCount=2,avgSalary=4000.0]
[Department Aggregate]: support, guru.learningjournal.kafka.examples.types.DepartmentAggregate@7869d13e[totalSalary=7000,employeeCount=1,avgSalary=7000.0]
[Department Aggregate]: support, guru.learningjournal.kafka.examples.types.DepartmentAggregate@7748b54c[totalSalary=13000,employeeCount=2,avgSalary=6500.0]
[Department Aggregate]: engineering, guru.learningjournal.kafka.examples.types.DepartmentAggregate@5353bfe3[totalSalary=3000,employeeCount=1,avgSalary=3000.0]
[Department Aggregate]: support, guru.learningjournal.kafka.examples.types.DepartmentAggregate@2125c3d7[totalSalary=18000,employeeCount=3,avgSalary=6000.0]
[Department Aggregate]: support, guru.learningjournal.kafka.examples.types.DepartmentAggregate@7b7cbf32[totalSalary=11000,employeeCount=2,avgSalary=5500.0]
[Department Aggregate]: engineering, guru.learningjournal.kafka.examples.types.DepartmentAggregate@40ca7f72[totalSalary=10000,employeeCount=2,avgSalary=5000.0]


 */