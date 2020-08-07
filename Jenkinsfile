pipeline {
    agent {label 'docker-slave-r5.2xlarge'}
    environment {
        GOOGLE_API_KEY = credentials('GOOGLE_API_KEY')
        GOOGLE_ADS_LOGIN_CUSTOMER_ID = credentials('GOOGLE_ADS_LOGIN_CUSTOMER_ID')
        GOOGLE_ADS_DEVELOPER_TOKEN = credentials('GOOGLE_ADS_DEVELOPER_TOKEN')
        GOOGLE_ADS_CLIENT_ID = credentials('GOOGLE_ADS_CLIENT_ID')
        GOOGLE_ADS_CLIENT_SECRET = credentials('GOOGLE_ADS_CLIENT_SECRET')
        GOOGLE_ADS_REFRESH_TOKEN = credentials('GOOGLE_ADS_REFRESH_TOKEN')
        GOOGLE_CM_API_USER_ID = credentials('GOOGLE_CM_API_USER_ID')
    }
    stages {
        stage('Build image') {
            steps {
                script {
                    withCredentials([
                            usernamePassword(credentialsId: 'datascience-harbor', usernameVariable: 'DOCKER_USR', passwordVariable: 'DOCKER_PW'),
                        ])
                    {
                    sh "docker login -u '${DOCKER_USR}' -p ${DOCKER_PW} https://harbor.aws.c.dk/"
                    image = docker.build("${JOB_NAME}:${BUILD_ID}")
                    }
                }
            }
        }
        stage('Make ingestion') {
            steps {
                script {
                    withCredentials([
                        usernamePassword(credentialsId: 'nuuday-ai-prod-mysql', usernameVariable: 'MARIADB_USR', passwordVariable: 'MARIADB_PSW'),
                        string(credentialsId: "GOOGLE_API_KEY", variable: 'GOOGLE_API_KEY'),
                        string(credentialsId: "GOOGLE_ADS_LOGIN_CUSTOMER_ID", variable: 'GOOGLE_ADS_LOGIN_CUSTOMER_ID'),
                        string(credentialsId: "GOOGLE_ADS_DEVELOPER_TOKEN", variable: 'GOOGLE_ADS_DEVELOPER_TOKEN'),
                        string(credentialsId: "GOOGLE_ADS_CLIENT_ID", variable: 'GOOGLE_ADS_CLIENT_ID'),
                        string(credentialsId: "GOOGLE_ADS_CLIENT_SECRET", variable: 'GOOGLE_ADS_CLIENT_SECRET'),
                        string(credentialsId: "GOOGLE_ADS_REFRESH_TOKEN", variable: 'GOOGLE_ADS_REFRESH_TOKEN'),
                        string(credentialsId: "GOOGLE_CM_API_USER_ID", variable: 'GOOGLE_CM_API_USER_ID'),
                        file(credentialsId: 'paid-media-db-ingestion-e95489028100.json', variable: 'gcm_credentialfile')
                    ]) {
                        withEnv([
                            "GOOGLE_ADS_PATH_TO_PRIVATE_KEY_FILE=/app/credentials/paid-media-db-ingestion-e95489028100.json",
                            "GOOGLE_ADS_DELEGATED_ACCOUNT=nuuday-paid@paid-media-db-ingestion.iam.gserviceaccount.com"
                        ]) {
                            sh "cp ${gcm_credentialfile} paid-media-db-ingestion-e95489028100.json"
                            sh "docker run -t --rm -e 'MARIADB_USR=${MARIADB_USR}' -e 'MARIADB_PSW=${MARIADB_PSW}' \
                                -e 'GOOGLE_API_KEY=${GOOGLE_API_KEY}' -e 'GOOGLE_ADS_LOGIN_CUSTOMER_ID=${GOOGLE_ADS_LOGIN_CUSTOMER_ID}' \
                                -e 'GOOGLE_ADS_DEVELOPER_TOKEN=${GOOGLE_ADS_DEVELOPER_TOKEN}' -e 'GOOGLE_ADS_CLIENT_ID=${GOOGLE_ADS_CLIENT_ID}' \
                                -e 'GOOGLE_ADS_CLIENT_SECRET=${GOOGLE_ADS_CLIENT_SECRET}' -e 'GOOGLE_ADS_REFRESH_TOKEN=${GOOGLE_ADS_REFRESH_TOKEN}' \
                                -e 'GOOGLE_CM_API_USER_ID=${GOOGLE_CM_API_USER_ID}' -e 'GOOGLE_ADS_PATH_TO_PRIVATE_KEY_FILE=${GOOGLE_ADS_PATH_TO_PRIVATE_KEY_FILE}' \
                                -e 'GOOGLE_ADS_DELEGATED_ACCOUNT=${GOOGLE_ADS_DELEGATED_ACCOUNT}' --build-arg GCM_CREDENTIAL_FILE=paid-media-db-ingestion-e95489028100.json \
                                '${JOB_NAME}':'${BUILD_ID}'"
                        }
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