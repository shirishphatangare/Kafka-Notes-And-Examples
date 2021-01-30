package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.DepartmentAggregate;
import guru.learningjournal.kafka.examples.types.Employee;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

/*  This example demonstrates KStream Aggregation using aggregate. However, this is an Update Stream scenario and hence ideally should be implemented
 *  using KTable
 *  Rule of Thumb --
 *  KTable - Update Stream scenario
 *  KStream - Append Stream scenario
 *
 *  You can observe differences in update scenario by executing and comparing resutls for 09-avg-salary-dept-kstream-aggregate-glitch and 11-avg-salary-dept-ktable-aggregate-improved
 *  */

public class KStreamAggDemo {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreLocation);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.stream(AppConfigs.topicName,
            Consumed.with(AppSerdes.String(), AppSerdes.Employee()))
            // Change key from Emp_Id to Department
            // For KTable groupBy returns a Key-Value Pair, whereas for KStream  groupBy returns a Key
            .groupBy((k,v) -> v.getDepartment(), Grouped.with(AppSerdes.String(),AppSerdes.Employee()))
            .aggregate(
                // 1) Initializer Argument - Set initial value for a State-Store. reduce() method auto-initializes since it knows input and output data types which are same.
                ()-> new DepartmentAggregate()
                .withEmployeeCount(0)
                .withTotalSalary(0)
                .withAvgSalary(0D),
                // 2) Aggregator Argument - Same as reduce operation
                (k,v,aggValue) ->
                    new DepartmentAggregate()
                .withEmployeeCount(aggValue.getEmployeeCount()+1)
                .withTotalSalary(aggValue.getTotalSalary() + v.getSalary())
                .withAvgSalary((aggValue.getTotalSalary()+v.getSalary())/(aggValue.getEmployeeCount()+1D)),
                // 3) Serializer Argument - State-Store name and Serdes
                Materialized.<String,DepartmentAggregate, KeyValueStore<Bytes, byte[]>>as(AppConfigs.stateStoreName)
                .withKeySerde(AppSerdes.String())
                .withValueSerde(AppSerdes.DepartmentAggregate())
            ).toStream().print(Printed.<String, DepartmentAggregate>toSysOut().withLabel("Department Salary Average"));


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

Observe below output - Updates not performed (records appended) since we are using KStream here

[Department Salary Average]: accounts, guru.learningjournal.kafka.examples.types.DepartmentAggregate@42fec134[totalSalary=8000,employeeCount=1,avgSalary=8000.0]
[Department Salary Average]: engineering, guru.learningjournal.kafka.examples.types.DepartmentAggregate@18fe80cd[totalSalary=5000,employeeCount=1,avgSalary=5000.0]
[Department Salary Average]: support, guru.learningjournal.kafka.examples.types.DepartmentAggregate@1b47dd0d[totalSalary=13000,employeeCount=2,avgSalary=6500.0]
[Department Salary Average]: engineering, guru.learningjournal.kafka.examples.types.DepartmentAggregate@3ab67a0b[totalSalary=8000,employeeCount=2,avgSalary=4000.0]
[Department Salary Average]: support, guru.learningjournal.kafka.examples.types.DepartmentAggregate@53f0210d[totalSalary=18000,employeeCount=3,avgSalary=6000.0]
[Department Salary Average]: engineering, guru.learningjournal.kafka.examples.types.DepartmentAggregate@20e20a85[totalSalary=15000,employeeCount=3,avgSalary=5000.0]



 */