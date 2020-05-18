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
        test_branch = 'bw_branch_7_7_2'
        branch_period = test_branch.replaceAll('_', '.')
        echo "branch_period..." + branch_period
        def regex_capture = branch_period =~ /\d.+/
        echo "regex_capture..." + regex_capture
        version = regex_capture[0]
        echo "version..." + version
    }
}
