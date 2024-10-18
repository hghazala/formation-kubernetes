FROM openjdk:17-jdk-slim
WORKDIR /app
COPY /target/formation-kube-0.0.1-SNAPSHOT.jar formation-kube.jar
EXPOSE 8080
# java -jar formation-kube.jar
ENTRYPOINT ["java", "-jar", "formation-kube.jar"]
