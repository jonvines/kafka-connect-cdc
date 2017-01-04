node {
    def mvnBuildNumber = "0.0.1.${env.BUILD_NUMBER}"
    def mvnHome = tool 'M3'

    checkout scm

    configFileProvider([configFile(fileId: 'mavenSettings', variable: 'maven_settings')]) {
        docker.image('maven:3.3.3-jdk-8').inside('--add-host nexus.custenborder.com:10.10.0.22') {
          sh "mvn -X -B -s ${maven_settings} clean package"
        }
    }
}

