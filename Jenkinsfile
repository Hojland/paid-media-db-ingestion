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
            withEnv([
                GOOGLE_API_KEY = credentials('GOOGLE_API_KEY')
                GOOGLE_ADS_LOGIN_CUSTOMER_ID = credentials('GOOGLE_ADS_LOGIN_CUSTOMER_ID')
                GOOGLE_ADS_DEVELOPER_TOKEN = credentials('GOOGLE_ADS_DEVELOPER_TOKEN')
                GOOGLE_ADS_CLIENT_ID = credentials('GOOGLE_ADS_CLIENT_ID')
                GOOGLE_ADS_CLIENT_SECRET = credentials('GOOGLE_ADS_CLIENT_SECRET')
                GOOGLE_ADS_REFRESH_TOKEN = credentials('GOOGLE_ADS_REFRESH_TOKEN')
                GOOGLE_CM_API_USER_ID = credentials('GOOGLE_CM_API_USER_ID')
                GOOGLE_ADS_PATH_TO_PRIVATE_KEY_FILE="/app/credentials/paid-media-db-ingestion-e95489028100.json"
                GOOGLE_ADS_DELEGATED_ACCOUNT="nuuday-paid@paid-media-db-ingestion.iam.gserviceaccount.com"
                    ]) {
                steps {
                    script {
                        withCredentials([
                            usernamePassword(credentialsId: 'nuuday-ai-prod-mysql', usernameVariable: 'MARIADB_USR', passwordVariable: 'MARIADB_PSW'),
                        ]) {
                            sh "docker run -t --rm -e 'MARIADB_USR=${MARIADB_USR}' -e 'MARIADB_PSW=${MARIADB_PSW}'
                                -e 'GOOGLE_API_KEY=${GOOGLE_API_KEY}' -e 'GOOGLE_ADS_LOGIN_CUSTOMER_ID=${GOOGLE_ADS_LOGIN_CUSTOMER_ID}' 
                                -e 'GOOGLE_ADS_DEVELOPER_TOKEN=${GOOGLE_ADS_DEVELOPER_TOKEN}' -e 'GOOGLE_ADS_CLIENT_ID=${GOOGLE_ADS_CLIENT_ID}'
                                -e 'GOOGLE_ADS_CLIENT_SECRET=${GOOGLE_ADS_CLIENT_SECRET}' -e 'GOOGLE_ADS_REFRESH_TOKEN=${GOOGLE_ADS_REFRESH_TOKEN}'
                                -e 'GOOGLE_CM_API_USER_ID=${GOOGLE_CM_API_USER_ID}' -e 'GOOGLE_ADS_PATH_TO_PRIVATE_KEY_FILE=${GOOGLE_ADS_PATH_TO_PRIVATE_KEY_FILE}'
                                -e 'GOOGLE_ADS_DELEGATED_ACCOUNT=${GOOGLE_ADS_DELEGATED_ACCOUNT}'
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