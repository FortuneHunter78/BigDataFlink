FROM flink:1.17.1-scala_2.12

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev wget && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install apache-flink==1.17.1

RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.0-1.17/flink-sql-connector-kafka-3.0.0-1.17.jar
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.1-1.17/flink-connector-jdbc-3.1.1-1.17.jar
RUN wget -P /opt/flink/lib/ https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
