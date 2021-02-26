#!groovy
gcpProject = "bw-prod-platform0"

def pushImage(String imageID, String imageVersion) {
    buildNumber = "build_${env.BUILD_NUMBER}"
    withEnv(["DOCKER_BUILDKIT=1"]) {
        img = docker.image(imageID)
        docker.withRegistry("https://eu.gcr.io", "gcr:${gcpProject}") {
            img.push(imageVersion)
            img.push(buildNumber)
        }
    }
}

node('docker') {
    stage ('Checkout') {
        checkout([
            $class: 'GitSCM',
            branches: scm.branches,
            doGenerateSubmoduleConfigurations: scm.doGenerateSubmoduleConfigurations,
            extensions: scm.extensions + [[$class: 'CleanCheckout']],
            userRemoteConfigs: scm.userRemoteConfigs
        ])

        versions = readJSON file: "versions.json"
        version = versions["version"]
        ant_version = versions["ant_version"]
    }

    stage ('Tests') {
        // NB the solr tests are flakey and take a long time to run
        // I've opted to skip bad apples (known flakey tests) and the
        // slow tests to keep things sane for now.  It's better to run
        // some tests than no tests, but be aware this may compromise
        // test coverage.
        sh "ant ivy-bootstrap"
        sh "ant -Dtests.badapples=false -Dtests.slow=false test"
    }

    stage ('Build images') {
        withEnv(["DOCKER_BUILDKIT=1"]) {
            sh """ docker build -f base.Dockerfile \
                --build-arg ANT_VERSION=${ant_version} \
                -t ${gcpProject}/ant-base:${ant_version} .
            """
            pushImage("${gcpProject}/ant-base:${ant_version}", ant_version)

            sh """ docker build \
                --build-arg REPO=${gcpProject} \
                --build-arg ANT_VERSION=${ant_version} \
                -t ${gcpProject}/bw-lucene-solr:${version} .
            """
            pushImage("${gcpProject}/bw-lucene-solr:${version}", version)
        }
    }
}
