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
        sh "ant ivy-bootstrap"
        sh "ant -Dtests.badapples=false test"
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
