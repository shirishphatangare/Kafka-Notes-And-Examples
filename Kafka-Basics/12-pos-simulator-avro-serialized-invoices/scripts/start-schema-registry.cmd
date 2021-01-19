rem With Confluent community edition I downloaded (confluent-6.0.1), I could not find script %KAFKA_HOME%\bin\windows\schema-registry-start.bat
rem I tried getting script from online source however got below error
rem Error: Could not find or load main class io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain

%KAFKA_HOME%\bin\windows\schema-registry-start.bat %KAFKA_HOME%\etc\schema-registry\schema-registry.properties

