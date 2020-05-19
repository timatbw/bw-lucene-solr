ARG REPO
ARG ANT_VERSION
FROM ${REPO}/ant-base:${ANT_VERSION}

COPY . /bw-lucene-solr/
WORKDIR /bw-lucene-solr/solr/
RUN ant ivy-bootstrap && \
    ant --noconfig -Dtests.badapples=false clean dist package
