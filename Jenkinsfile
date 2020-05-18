#!groovy
def gcpProject =  "bw-prod-platform0"
def baseImageExist = false

node('docker') {
    stage ('Checkout') {
        checkout([
            $class: 'GitSCM',
            branches: scm.branches,
            doGenerateSubmoduleConfigurations: scm.doGenerateSubmoduleConfigurations,
            extensions: scm.extensions + [[$class: 'CleanCheckout']],
            userRemoteConfigs: scm.userRemoteConfigs
        ])

        // Could change this to be held within a versions.json file if preferred
        // def branch = env.BRANCH_NAME
        // TODO - change this when no longer using this branch name
        test_branch = 'bw_branch_7_7_2'
        branch_period = test_branch.replaceAll('_', '.')
        def regex_capture = branch_period =~ /\d.+/
        version = regex_capture[0]
        major_version = version.substring(0,1)
    }

    stage ('Build images') {
        baseImageExist = doesBaseImageExist(major_version)
        if ( baseImageExist ) {
            withEnv(["DOCKER_BUILDKIT=1"]) {
                sh """ docker build \
                    -f Dockerfile \
                    --build-arg REPO=${gcpProject} \
                    --build-arg MAJOR_VERSION=${major_version} \
                    -t ${gcpProject}/bw-lucene-solr:${version} .
                """
            }
        } else {
            withEnv(["DOCKER_BUILDKIT=1"]) {
                sh "docker build -f base.Dockerfile -t ${gcpProject}/ant-base:${major_version} ."
                sh """ docker build \
                    -f Dockerfile \
                    --build-arg REPO=${gcpProject} \
                    --build-arg MAJOR_VERSION=${major_version} \
                    -t ${gcpProject}/bw-lucene-solr:${version} .
                """
            }
        }
    }
}

def doesBaseImageExist(String imageVersion) {
    try {
        img = docker.image("${gcpProject}/ant-base:${imageVersion}").withRun { c ->
            sh 'ant --help'
        }
        return true
    } catch (Exception e) {
        return false
    }
}
