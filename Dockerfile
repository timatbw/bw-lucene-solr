ARG REPO=""
ARG MAJOR_VERSION=latest
FROM ${REPO}/ant-base:${MAJOR_VERSION}

ADD . /bw-lucene-solr/
WORKDIR /bw-lucene-solr/solr/
RUN ls
RUN ant ivy-bootstrap && \
    ant clean compile dist package
