@Library('jenkinsPipeline')_


properties([
  parameters([
     booleanParam(name: 'DEPLOY_TEMPLATES', defaultValue: false, description: 'Opt to deploy Dataflow and Airflow templates'),
     booleanParam(name: 'PERFORM_SCANS', defaultValue: true, description: 'Opt to perform Sonar, NexusIQ, Fortify scans')
  ])
])

def buildArguments(envArgs, templateArgs){
    def arguments = ""
    envArgs.each { key, value ->
        arguments += "--${key}=${value} "
    }
    templateArgs.each { key, value ->
            arguments += "--${key}=${value} "
    }
    return arguments.trim();
}

def getconfig(envProps){
    if(env.BRANCH_NAME == "master"){
        return envProps['prod']
    }else if(env.BRANCH_NAME == "release"){
        return envProps['test']
    }else{
        return envProps['dev']
    }
}

if(params.PERFORM_SCANS && env.BRANCH_NAME == 'develop'){
    env.enableFortify=true
    echo 'enabled fortify scan....'
    standardGCPPipeline(email: 'saphalya.swain@equifax.com')
}else if(params.PERFORM_SCANS){
    standardGCPPipeline(email: 'saphalya.swain@equifax.com')
}else{
    echo 'Skipping scan stages.....'
}

node{
	if(params.DEPLOY_TEMPLATES){
		node("dibi-batch-pod") {
			try{
				def envProps = null
				stage("Initialize Environment"){
					checkout scm
					envProps = readJSON file: 'dataflow-config.json'
				}
				stage("Create Dataflow Template"){
					def envConfig = null
					envConfig = getconfig(envProps)
					container('mvn36jdk11') {
						configFileProvider([configFile(fileId: "defaultMVNSettings", variable: 'MAVEN_SETTINGS')])
						{
							def templates = envConfig['templates']
							def envArgs = envConfig['arguments']
							for(template in templates){
								def templateArgs = template['arguments']
								def mainClass = template['mainClass']
								def templateName = template['templateName']
								echo "Creating template ${templateName}......"
								def arguments = buildArguments(envArgs, templateArgs)
								def files = findFiles(glob: '**/*bundled*.jar')
								sh "mvn -Pdataflow-runner compile exec:java -s $MAVEN_SETTINGS -Dexec.mainClass=${mainClass} -Dexec.args='${arguments}'"
							}
						}
					}
				}
				stage("Deploy Airflow Templates"){
					def envConfig = null
					envConfig = getconfig(envProps)
					sh "gsutil cp ./airflow/*.py ${envConfig['dagConfig']['dagsFolder']}"
				}
			}catch(e){
				echo 'Template Deployement failed'
                throw e;
			}finally{
				deleteDir()
			}
		}
	}else{
		echo 'Deploy template stages opted out...skipping deploy template stages'
	}
}