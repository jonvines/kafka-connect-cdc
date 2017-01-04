node {
    def mvnBuildNumber = "0.0.1.${env.BUILD_NUMBER}"
    def mvnHome = tool 'M3'

    checkout scm

    configFileProvider([configFile(fileId: 'mavenSettings', variable: 'maven_settings')]) {
        docker.image('maven:3.3.3-jdk-8').inside {
          sh "mvn -B -s ${maven_settings} clean package"
        }
    }
}

