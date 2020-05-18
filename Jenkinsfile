#!groovy

node('docker') {
    stage ('Checkout') {
        checkout([
            $class: 'GitSCM',
            branches: scm.branches,
            doGenerateSubmoduleConfigurations: scm.doGenerateSubmoduleConfigurations,
            extensions: scm.extensions + [[$class: 'CleanCheckout']],
            userRemoteConfigs: scm.userRemoteConfigs
        ])
        branch = env.BRANCH_NAME
        // TODO - change this when no longer using this branch name
        test_branch = 'bw_branch_7_7_2'
        branch_period = test_branch.replaceAll('_', '.')
        def regex_capture = branch_period =~ /\d.+/
        version = regex_capture[0]
    }

    stage ('Build and push images') {
        withEnv(["DOCKER_BUILDKIT=1"]) {
            sh "docker build -f Dockerfile --build-arg VERSION=${version} ."
        }
    }
}
