FROM raizenanalyticsdev.azurecr.io/airflow-base:latest

RUN pip install --user apache-airflow[hive,jdbc]

RUN pip install --user Flask==1.0.3

USER root

# install libxinerama1 for libreoffice6.2
RUN apt-get update
# install Hadoop
RUN apt-get install -y openjdk-8-jre-headless
RUN apt-get install -y openjdk-8-jdk-headless

#ENV JAVA_HOME "/usr/lib/jvm/default-java"
ENV JAVA_HOME "/usr/lib/jvm/java-1.8.0-openjdk-amd64"
ARG HADOOP_VERSION="3.1.1"
ENV HADOOP_HOME "/opt/hadoop"
RUN curl https://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    | tar xz -C /opt && mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME}
ENV HADOOP_COMMON_HOME "${HADOOP_HOME}"
ENV HADOOP_CLASSPATH "${HADOOP_HOME}/share/hadoop/tools/lib/*"
ENV HADOOP_CONF_DIR "${HADOOP_HOME}/etc/hadoop"
ENV PATH "$PATH:${HADOOP_HOME}/bin"
ENV HADOOP_OPTS "$HADOOP_OPTS -Djava.library.path=${HADOOP_HOME}/lib"
ENV HADOOP_COMMON_LIB_NATIVE_DIR "${HADOOP_HOME}/lib/native"
ENV YARN_CONF_DIR "${HADOOP_HOME}/etc/hadoop"

# install Spark
ARG SPARK_VERSION="2.3.1"
ARG PY4J_VERSION="0.10.7"
ENV SPARK_HOME "/opt/spark"
RUN curl https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz \
    | tar xz -C /opt && mv /opt/spark-${SPARK_VERSION}-bin-without-hadoop ${SPARK_HOME}
ENV PATH "$PATH:${SPARK_HOME}/bin"
ENV LD_LIBRARY_PATH "${HADOOP_HOME}/lib/native"
ENV SPARK_DIST_CLASSPATH "${HADOOP_HOME}/etc/hadoop\
:${HADOOP_HOME}/share/hadoop/common/lib/*\
:${HADOOP_HOME}/share/hadoop/common/*\
:${HADOOP_HOME}/share/hadoop/hdfs\
:${HADOOP_HOME}/share/hadoop/hdfs/lib/*\
:${HADOOP_HOME}/share/hadoop/hdfs/*\
:${HADOOP_HOME}/share/hadoop/yarn/lib/*\
:${HADOOP_HOME}/share/hadoop/yarn/*\
:${HADOOP_HOME}/share/hadoop/mapreduce/lib/*\
:${HADOOP_HOME}/share/hadoop/mapreduce/*\
:${HADOOP_HOME}/share/hadoop/tools/lib/*\
:${HADOOP_HOME}/contrib/capacity-scheduler/*.jar"
ENV PYSPARK_PYTHON "/usr/local/bin/python"
ENV PYTHONPATH "${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-${PY4J_VERSION}-src.zip:${PYTHONPATH}"
ENV SPARK_OPTS "--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info"


RUN pip install pyspark==2.3.1
RUN pip install findspark
RUN pip install pyarrow==0.14.1

USER engineer
WORKDIR /home/engineer/

RUN pip install --user numpy
RUN pip install --user pandas
RUN pip install --user JPype1==0.6.3
RUN pip install --user jaydebeapi
RUN pip install --user joblib
RUN pip install --user pyIpeaData

RUN pip install --user great_expectations

USER root
COPY ./libs/mssql-jdbc-7.2.2.jre8.jar ./libs/mssql-jdbc-7.2.2.jre8.jar
RUN chown engineer:engineer ./libs/mssql-jdbc-7.2.2.jre8.jar

USER engineer
WORKDIR /home/engineer/
