rm -rf ./target \
 && mvn clean package assembly:single \
 && mv ./target/dmp-1.0-jar-with-dependencies.jar dmp-v1.jar