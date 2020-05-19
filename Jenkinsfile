#!groovy
gcpProject = "bw-prod-platform0"
baseImageExists = false

def doesBaseImageExist(String imageVersion) {
    /*
        Originally, the docker file would create a new debian image and install
        all the dependencies needed to build solr, to save time, I've separated
        the two "stages" into separate docker files, with this, I then do a check
        to see if the ant-base image exists in our gcr, if it does, then it'll skip
        building it which should cut down on processing time
    */
    try {
        img = docker.image("${gcpProject}/ant-base:${imageVersion}").withRun { c ->
            sh 'ant --help'
        }
        return true
    } catch (Exception e) {
        return false
    }
}

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

    stage ('Build images') {
        baseImageExists = doesBaseImageExist(ant_version)
        if ( !baseImageExists ) {
            echo "Unable to find base image, building"
            withEnv(["DOCKER_BUILDKIT=1"]) {
                sh """ docker build -f base.Dockerfile \
                    --build-arg ANT_VERSION=${ant_version} \
                    -t ${gcpProject}/ant-base:${ant_version} .
                """
                pushImage("${gcpProject}/ant-base:${ant_version}", ant_version)
            }
        }
        withEnv(["DOCKER_BUILDKIT=1"]) {
            sh """ docker build \
                --build-arg REPO=${gcpProject} \
                --build-arg ANT_VERSION=${ant_version} \
                -t ${gcpProject}/bw-lucene-solr:${version} .
            """
            pushImage("${gcpProject}/bw-lucene-solr:${version}", version)
        }
    }
}
