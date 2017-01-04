node {
    def mvnBuildNumber = "0.0.1.${env.BUILD_NUMBER}"
    def mvnHome = tool 'M3'

    checkout scm

    docker.image('maven:3.3.3-jdk-8').inside {
      sh 'mvn -B clean package'
    }
}