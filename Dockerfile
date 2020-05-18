FROM debian:stretch

ARG VERSION=0

RUN echo "deb http://http.debian.net/debian stretch-backports main" | \
     tee --append /etc/apt/sources.list.d/stretch-backports.list > /dev/null && \ 
     apt-get update -y && \
     apt-get install -y \
     wget \
     curl \
     ruby \
     ruby-dev \
     rubygems \
     build-essential \
     git-core && \
     apt-get install -y -t stretch-backports openjdk-8-jdk && \
     update-java-alternatives -s java-1.8.0-openjdk-amd64 && \
     apt-get upgrade -y

RUN export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64

RUN gem install fpm
RUN fpm --version

#Installing Apache Ant
RUN wget http://archive.apache.org/dist/ant/binaries/apache-ant-1.10.5-bin.tar.gz
RUN wget https://www.apache.org/dist/ant/KEYS
RUN wget https://www.apache.org/dist/ant/binaries/apache-ant-1.10.5-bin.tar.gz.asc

#Verify download signature
RUN gpg --import KEYS
RUN gpg --verify apache-ant-1.10.5-bin.tar.gz.asc

#Unpack
RUN tar xvfvz apache-ant-1.10.5-bin.tar.gz

RUN mv apache-ant-1.10.5 /opt/ant
RUN echo ANT_HOME=/opt/ant >> /etc/environment
RUN ln -s /opt/ant/bin/ant /usr/bin/ant

#Installing Apache Ivy from git repository
RUN git clone https://git-wip-us.apache.org/repos/asf/ant-ivy.git
WORKDIR /ant-ivy
RUN ant jar
WORKDIR /ant-ivy/build/artifact/jars
RUN scp ivy.jar /opt/ant/lib

WORKDIR /solr-main/solr/
RUN ant ivy-bootstrap && \
    ant clean compile dist package
