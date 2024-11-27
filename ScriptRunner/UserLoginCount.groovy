import com.atlassian.jira.component.ComponentAccessor
import com.atlassian.jira.security.login.LoginManager

def loginManager = ComponentAccessor.getComponent(LoginManager)
def loginDetails = loginManager.getLoginInfo("u1002018")

loginDetails.getLoginCount()


import org.apache.log4j.Logger
import org.apache.log4j.Level

def log= Logger.getLogger()
log.setLevel(Level.DEBUG)