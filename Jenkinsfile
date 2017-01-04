node {
    def jdk8_docker_image = 'maven:3.3.3-jdk-8'
    def maven_build_number = "0.0.1.${env.BUILD_NUMBER}"

    checkout scm

    withCredentials([
        usernamePassword(credentialsId: 'gpg_passphrase',
        passwordVariable: 'gpg_passphrase', usernameVariable: 'gpg_key'),
        file(credentialsId: 'gpg_pubring', variable: 'gpg_pubring'),
        file(credentialsId: 'gpg_secring', variable: 'gpg_secring')]
    ) {
        configFileProvider([configFile(fileId: 'mavenSettings', variable: 'maven_settings')]) {
            docker.image(jdk8_docker_image).inside {
                sh "mvn -B -s ${maven_settings} -Dgpg.keyname=${gpg_key} -Dgpg.passphraseServerId=${gpg_key} -Dgpg.publicKeyring=${gpg_pubring} -Dgpg.secretKeyring=${gpg_secring} clean package"
            }
        }
    }
}

