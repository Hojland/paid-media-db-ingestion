pipeline {
    agent {label 'docker-slave-r5.2xlarge'}
    stages {
        stage('Build image') {
            steps {
                script {
                    withCredentials([
                            file(credentialsId: 'paid-media-db-ingestion-e95489028100.json', variable: 'gcm_credentialfile')
                        ])
                    {
                    sh "cp ${gcm_credentialfile} paid-media-db-ingestion-e95489028100.json"
                    image = docker.build("${JOB_NAME}:${BUILD_ID}", '--build-arg GCM_CREDENTIAL_FILE=paid-media-db-ingestion-e95489028100.json .')
                    }
                }
            }
        }
        stage('Make ingestion') {
            steps {
                script {
                    withCredentials([
                        usernamePassword(credentialsId: 'nuuday-ai-prod-mysql', usernameVariable: 'MARIADB_USR', passwordVariable: 'MARIADB_PSW'),
                        string(credentialsId: "GOOGLE_API_KEY", variable: "GOOGLE_API_KEY"),
                        string(credentialsId: "GOOGLE_ADS_LOGIN_CUSTOMER_ID", variable: 'GOOGLE_ADS_LOGIN_CUSTOMER_ID'),
                        string(credentialsId: "GOOGLE_ADS_DEVELOPER_TOKEN", variable: 'GOOGLE_ADS_DEVELOPER_TOKEN'),
                        string(credentialsId: "GOOGLE_ADS_CLIENT_ID", variable: 'GOOGLE_ADS_CLIENT_ID'),
                        string(credentialsId: "GOOGLE_ADS_CLIENT_SECRET", variable: 'GOOGLE_ADS_CLIENT_SECRET'),
                        string(credentialsId: "GOOGLE_ADS_REFRESH_TOKEN", variable: 'GOOGLE_ADS_REFRESH_TOKEN'),
                        string(credentialsId: "FACEBOOK_APP_ID", variable: 'FACEBOOK_APP_ID'),
                        string(credentialsId: "FACEBOOK_APP_SECRET", variable: 'FACEBOOK_APP_SECRET'),
                        string(credentialsId: "FACEBOOK_API_TOKEN_2", variable: 'FACEBOOK_ACCESS_TOKEN'),
                    ]) {
                            sh "docker run -t --rm -e 'MARIADB_USR=${MARIADB_USR}' -e 'MARIADB_PSW=${MARIADB_PSW}' \
                                -e 'GOOGLE_API_KEY=${GOOGLE_API_KEY}' -e 'GOOGLE_ADS_LOGIN_CUSTOMER_ID=${GOOGLE_ADS_LOGIN_CUSTOMER_ID}' \
                                -e 'GOOGLE_ADS_DEVELOPER_TOKEN=${GOOGLE_ADS_DEVELOPER_TOKEN}' -e 'GOOGLE_ADS_CLIENT_ID=${GOOGLE_ADS_CLIENT_ID}' \
                                -e 'GOOGLE_ADS_CLIENT_SECRET=${GOOGLE_ADS_CLIENT_SECRET}' -e 'GOOGLE_ADS_REFRESH_TOKEN=${GOOGLE_ADS_REFRESH_TOKEN}' \
                                -e 'FACEBOOK_APP_ID=${FACEBOOK_APP_ID}' -e 'FACEBOOK_APP_SECRET=${FACEBOOK_APP_SECRET}' \
                                -e 'FACEBOOK_ACCESS_TOKEN=${FACEBOOK_ACCESS_TOKEN}' \
                                '${JOB_NAME}':'${BUILD_ID}'"
                    }
                }
            }
        }
    }
    post {
        always {
            cleanWs()
        }
    }
}