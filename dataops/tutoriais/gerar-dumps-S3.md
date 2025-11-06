# TASK
## Gerar dump da clients no S3
```
Daí esse código poderia ser chamado pelo Jenkins como um script ou algo similar:

#!/bin/bash

DATE=YYYY-MM-DD

rm -rf $FOLDER_TEMP/tmp

python "clientsdumper.py"

gzip $FOLDER_TEMP/tmp*

## as bibliotecas da aws tem retry builtin em geral, mas caso queira dá pra fazer tolerante a falha aqui
aws s3 cp tmp/ s3://platform-dumps-virginia/client/$CURRENTDAY/ --recursive

bruno.rozza@chaordicsystems.com, são apenas sugestões, vc é quem decide no final melhor forma de fazer. Isso precisa ir pra receita do chef, uma vez que se precisamos mudar de máquina o Job já vai automático.
```
## Resolution

- Create script python
- Create script bash
- Download platform-dump
- Insert repository with scripts python and bash in platform-dumps
- Git push auxiliar-branch


- Run machine jenkins: `ssh jenkins.platform.chaordicsystems.com`
- in machine jenkins: `cd /opt/platform-dumps/`
- git clone auxiliar-branch
- create job in site jenkins http://jenkins.platform.chaordicsystems.com:8080/. 
ex<br/>
`bash -l -c "cd /opt/platform-dumps/dump-clients-s3/; ./clientsdumper-jenkins.sh"`
- jenkins create jobs in `cd /mnt/jenkins/jobs/`

- Insert job in recipe in chef:
  - `cd /mnt/jenkins/jobs/`
  -  cat config.xml
  - copy: <br/>
```
<project>
  <actions/>
  <description></description>
  <keepDependencies>false</keepDependencies>
  <properties>
    <com.chikli.hudson.plugin.naginator.NaginatorOptOutProperty plugin="naginator@1.17.2">
      <optOut>false</optOut>
    </com.chikli.hudson.plugin.naginator.NaginatorOptOutProperty>
  </properties>
  <scm class="hudson.scm.NullSCM"/>
  <canRoam>true</canRoam>
  <disabled>false</disabled>
  <blockBuildWhenDownstreamBuilding>false</blockBuildWhenDownstreamBuilding>
  <blockBuildWhenUpstreamBuilding>false</blockBuildWhenUpstreamBuilding>
  <triggers/>
  <concurrentBuild>false</concurrentBuild>
  <builders>
    <hudson.tasks.Shell>
      <command>bash -l -c &quot;cd /opt/platform-dumps/gerar-dump-clients-s3/; ./clientsdumper-jenkins.sh&quot;</command>
    </hudson.tasks.Shell>
  </builders>
  <publishers>
    <hudson.tasks.Mailer plugin="mailer@1.20">
      <recipients>platform@chaordicsystems.com</recipients>
      <dontNotifyEveryUnstableBuild>false</dontNotifyEveryUnstableBuild>
      <sendToIndividuals>false</sendToIndividuals>
    </hudson.tasks.Mailer>
    <com.chikli.hudson.plugin.naginator.NaginatorPublisher plugin="naginator@1.17.2">
      <regexpForRerun></regexpForRerun>
      <rerunIfUnstable>true</rerunIfUnstable>
      <rerunMatrixPart>false</rerunMatrixPart>
      <checkRegexp>false</checkRegexp>
      <regexpForMatrixStrategy>TestParent</regexpForMatrixStrategy>
      <delay class="com.chikli.hudson.plugin.naginator.ProgressiveDelay">
        <increment>120</increment>
        <max>360</max>
      </delay>
      <maxSchedule>4</maxSchedule>
    </com.chikli.hudson.plugin.naginator.NaginatorPublisher>
  </publishers>
  <buildWrappers/>
</project>
  ```
  - alter file `attributes/default.rb` in # Jobs insert name job, ex `DumpClientsS3`
  
#### in machine local
- create file in `platform-chef-repo/chaordic-cookbooks/platform-jenkins/templates/jobs` with o name NAMEDUMP.xml.erb
- insert in file content: `jenkins: /mnt/jenkins/jobs/config.xml`

#### in jenkins:
- enter with user jenkins: `sudo su - jenkins`
- execute first in s3://giancarlo/teste-bruno
- execute in bucket correct
- execute in site jenkins http://jenkins.platform.chaordicsystems.com:8080/
