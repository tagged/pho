@GrabResolver('https://artifactory.tagged.com/artifactory/libs-release-local/')
@Grab('com.tagged.build:jenkins-dsl-common:0.1.22')

import com.tagged.build.common.*

def project = new Project(jobFactory,
    [
        githubOwner: 'chat',
        githubProject: 'morice',
        hipchatRoom: 'chatstore',
        email: 'web@tagged.com',
    ]
)

def build = project.basicJob {
    name 'build'
    logRotator(-1, 25)
    jdk 'jdk 6u43'
    label 'scala'

    steps {
        sbt(
            'sbt',
            'package',
            ('-Dsbt.log.noformat=true' +
             '-XX:PermSize=256M ' +
             '-XX:MaxPermSize=512M')
        )
    }
}
