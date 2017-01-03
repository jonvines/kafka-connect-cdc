node {
    def mvnBuildNumber = "0.0.1.${env.BUILD_NUMBER}"
    def mvnHome = tool 'M3'

    checkout scm

    sh "${mvnHome}/bin/mvn -B clean verify package"
}