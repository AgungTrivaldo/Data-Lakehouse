FROM bitnami/spark:3.3.2

USER root

COPY stock_transform.py /app/

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
    && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar \
    && mv hadoop-aws-3.3.2.jar /opt/bitnami/spark/jars/ \
    && mv aws-java-sdk-bundle-1.11.1026.jar /opt/bitnami/spark/jars/

ENV AWS_ACCESS_KEY_ID=admin123
ENV AWS_SECRET_ACCESS_KEY=admin123
ENV ENDPOINT=http://host.docker.internal:9000

WORKDIR /app

CMD ["spark-submit", \
    "--master", "local[*]", \
    "--jars", "/opt/bitnami/spark/jars/hadoop-aws-3.3.2.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.1026.jar", \
    "stock_transform.py", \
    "stock-market/AAPL/prices.json"]