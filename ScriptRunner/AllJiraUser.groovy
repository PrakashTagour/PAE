import com.atlassian.jira.component.ComponentAccessor
import com.atlassian.jira.security.login.LoginManager
import java.text.SimpleDateFormat;
import java.util.Date;
import com.atlassian.jira.user.util.UserUtil

import org.apache.log4j.Logger
import org.apache.log4j.Level

def log= Logger.getLogger("user Loin deatils")
log.setLevel(Level.DEBUG)

UserUtil userUtil = ComponentAccessor.getUserUtil()
def loginManager = ComponentAccessor.getComponentOfType(LoginManager.class)
// def users=ComponentAccessor.UserManager.getAllApplicationUsers()

def groupManager = ComponentAccessor.getGroupManager()

def jiraAssociates = groupManager.getUsersInGroup('JIRA (Associates)')
def jiraContractors = groupManager.getUsersInGroup('JIRA (Contractors)')
def jiraVendors = groupManager.getUsersInGroup('JIRA (Vendors)')

// def portfolioforJIRA = groupManager.getUsersInGroup('Portfolio_for_JIRA')
// def confluencAssociates  = groupManager.getUsersInGroup('Confluence (Associates)')
// def confluencContractors = groupManager.getUsersInGroup('Confluence (Contractors)')
// def confluencVendors = groupManager.getUsersInGroup('Confluence (Vendors)')

def users =  jiraAssociates + jiraContractors +jiraVendors
users.unique()
int count =0
StringBuilder builder=new StringBuilder()
builder.append("<table border = 1><tr><td><b>Count</b></td><td><b>User Name</b></td><td><b>Full Name</b></td><td><b>eMail Address</b></td><td><b>Last Login</b></td><td><b>LoginCount</b></td><td><b>Status</b></td></tr>")
users.each{
    count = count + 1
    Long lastLoginTime = loginManager.getLoginInfo(it.username).getLastLoginTime()
    Long loginCount = loginManager.getLoginInfo(it.username).getLoginCount()
    String activeStatus=it.active
    if(userUtil.getGroupsForUser(it.getName()).size() == 0) 
        //builder.append("<tr><td>"+it.username+"</td><td>"+it.displayName+"</td><td>"+it.emailAddress+"</td><td>No Group added</td><td>"+it.active+"</td></tr>")
    return
    // else if(activeStatus=="false")
    //     builder.append("<tr><td>"+count+"</td><td>"+it.username+"</td><td>"+it.displayName+"</td><td>"+it.emailAddress+"</td><td>Inactive User</td><td>"+it.active+"</td></tr>")
    // else if(lastLoginTime==null)
    //     builder.append("<tr><tb>"+count+"</td><td>"+it.username+"</td><td>"+it.displayName+"</td><td>"+it.emailAddress+"</td><td>Logon not found</td><td>"+it.active+"</td></tr>")
    else if (activeStatus=="true" && lastLoginTime != null ){
        Date date=new Date(lastLoginTime);
        SimpleDateFormat df2 = new SimpleDateFormat("dd/MM/yy hh:mm");
        String dateText = df2.format(date);
        builder.append("<tr><td>"+count+"</td><td>"+it.username+"</td><td>"+it.displayName+"</td><td>"+it.emailAddress+"</td><td>"+dateText+"</td><td>"+loginCount+"</td><td>"+it.active+"</td></tr>")
    }

}
builder.append("</table>")
return builder

log.debug(builder)
