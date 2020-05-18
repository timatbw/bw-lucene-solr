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
        test = sh(script: 'printenv', returnStdout: true)
        echo "${test}"
        echo "Test: ..." + env.BRANCH_NAME
    }
}
