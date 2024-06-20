mvn clean install jib:dockerBuild

docker run \
  -v /path/to/local/application.yml:/config/application.yml \
  -e SPRING_CONFIG_LOCATION=optional:classpath:/,file:/config/application.yml \
  -p 8080:8080 \
  eu.cymo/avro-composer:latest

  
See src/main/resources/application-example.yml on how to build your configuration.



TESTCASES TODO

adapting union fields:
- [null, record]
- [null, array]
schema evolution + mix of order of versions