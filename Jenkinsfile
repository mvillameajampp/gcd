@Library('jampp-shared-libraries@v1.0.3') _

pipeline {
    agent any
    environment {
        PYTHONTEST_IMAGE_VERSION = "3.2.0-python3.6"
    }
    stages {
      // Gcd tests
        stage("gcd_ci") {
            steps {
                script {
                    docker.image(
                        "docker.jampp.com/pythontest-image-builder:${PYTHONTEST_IMAGE_VERSION}"
                    ).inside("-v ${WORKSPACE}:/src -e EXTRA_REQUIRES_ALL=true ")  {
                        sh "/docker-entrypoint.sh pytest_coverage"
                    }
                }
                junit 'output.xml'
                cobertura autoUpdateHealth: false,
                autoUpdateStability: false,
                coberturaReportFile: 'coverage.xml',
                failUnhealthy: false,
                failUnstable: false,
                maxNumberOfBuilds: 0,
                onlyStable: false,
                sourceEncoding: 'ASCII',
                zoomCoverageChart: false
            }
        }
    }
}
