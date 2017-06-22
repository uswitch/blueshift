FROM azul/zulu-openjdk:8

RUN mkdir -p /blueshift
COPY logback.xml target/*.jar /blueshift/

CMD ["java", "-Xmx2G", "-Dlogback.configurationFile=/blueshift/logback.xml", "-server", "-cp", "/blueshift/*", "uswitch.blueshift.main"]
