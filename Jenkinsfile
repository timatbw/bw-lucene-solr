#!groovy
gcpProject =  "bw-prod-platform0"
baseImageExist = false

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
    withEnv(["DOCKER_BUILDKIT=1"]) {
        img = docker.image(imageID)
        docker.withRegistry("https://eu.gcr.io", "gcr:${gcpProject}") {
            img.push(imageVersion)
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
        major_version = versions["major_version"]
    }

    stage ('Build images') {
        baseImageExist = doesBaseImageExist(major_version)
        if ( baseImageExist ) {
            echo "Base image exists, using that"
            withEnv(["DOCKER_BUILDKIT=1"]) {
                sh """ docker build \
                    -f Dockerfile \
                    --build-arg REPO=${gcpProject} \
                    --build-arg MAJOR_VERSION=${major_version} \
                    -t ${gcpProject}/bw-lucene-solr:${version} .
                """
                pushImage("${gcpProject}/bw-lucene-solr:${version}", version)
            }
        } else {
            echo "Unable to find base image, building"
            withEnv(["DOCKER_BUILDKIT=1"]) {
                sh "docker build -f base.Dockerfile -t ${gcpProject}/ant-base:${major_version} ."
                pushImage("${gcpProject}/ant-base:${major_version}", major_version)

                sh """ docker build \
                    -f Dockerfile \
                    --build-arg REPO=${gcpProject} \
                    --build-arg MAJOR_VERSION=${major_version} \
                    -t ${gcpProject}/bw-lucene-solr:${version} .
                """
                pushImage("${gcpProject}/bw-lucene-solr:${version}", version)
            }
        }
    }
}
