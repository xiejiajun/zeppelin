### 使用OpenLDAP作为账号系统时的配置文档
#### 不需要修改源码的方式
```bash
[users]
#user3 = password4, role2

# Sample LDAP configuration, for user Authentication, currently tested for single Realm
[main]
ldapRealm=org.apache.zeppelin.realm.LdapRealm
ldapRealm.contextFactory.authenticationMechanism=simple
ldapRealm.contextFactory.url=ldap://ldap_host:389
ldapRealm.userDnTemplate=cn={0},ou=zeppelin,dc=hadoop,dc=apache,dc=org
ldapRealm.pagingSize = 200
ldapRealm.authorizationEnabled=true
ldapRealm.searchBase=dc=hadoop,dc=apache,dc=org
ldapRealm.userSearchBase = dc=hadoop,dc=apache,dc=org
ldapRealm.groupSearchBase = ou=groups,dc=hadoop,dc=apache,dc=org
ldapRealm.groupObjectClass=posixGroup
ldapRealm.userLowerCase = true
ldapRealm.memberAttribute = memberuid
ldapRealm.groupSearchFilter=(&(objectClass=posixGroup)(memberuid={0}))
ldapRealm.userSearchScope = subtree
ldapRealm.groupSearchScope = subtree
ldapRealm.contextFactory.systemUsername= cn=admin,dc=hadoop,dc=apache,dc=org
ldapRealm.contextFactory.systemPassword= <cn=admin,dc=hadoop,dc=apache,dc=org的密码>
ldapRealm.groupSearchEnableMatchingRuleInChain = false
ldapRealm.rolesByGroup = admin_group:admin_role,ldap_group1:zeppelin_role1,ldap_group2:zeppelin_role2
#ldapRealm.allowedRolesForAuthentication = admin_role,zeppelin_role1
securityManager.realms = $ldapRealm

sessionManager = org.apache.shiro.web.session.mgt.DefaultWebSessionManager

### Enables 'HttpOnly' flag in Zeppelin cookies
cookie = org.apache.shiro.web.servlet.SimpleCookie
cookie.name = JSESSIONID
cookie.httpOnly = true
### Uncomment the below line only when Zeppelin is running over HTTPS
#cookie.secure = true
sessionManager.sessionIdCookie = $cookie

securityManager.sessionManager = $sessionManager
# 86,400,000 milliseconds = 24 hour
securityManager.sessionManager.globalSessionTimeout = 86400000
shiro.loginUrl = /api/login

[roles]
role1 = *
role2 = *
role3 = *
admin = *

[urls]
# This section is used for url-based security. For details see the shiro.ini documentation.
#
# You can secure interpreter, configuration and credential information by urls.
# Comment or uncomment the below urls that you want to hide:
# anon means the access is anonymous.
# authc means form based auth Security.
#
# IMPORTANT: Order matters: URL path expressions are evaluated against an incoming request
# in the order they are defined and the FIRST MATCH WINS.
#
# To allow anonymous access to all but the stated urls,
# uncomment the line second last line (/** = anon) and comment the last line (/** = authc)
#
/api/version = anon
/api/cluster/address = anon
# Allow all authenticated users to restart interpreters on a notebook page.
# Comment out the following line if you would like to authorize only admin users to restart interpreters.
/api/interpreter/setting/restart/** = authc
/api/interpreter/** = authc, roles[admin]
/api/notebook-repositories/** = authc, roles[admin]
/api/configurations/** = authc, roles[admin]
/api/credential/** = authc, roles[admin]
/api/admin/** = authc, roles[admin]
#/** = anon
/** = authc
```
- 关键配置项：至于为啥说这些配置是关键项，看LdapRealm.rolesFor和
    - ldapRealm.groupSearchEnableMatchingRuleInChain = false
    - ldapRealm.memberAttribute = memberuid
    - ldapRealm.groupSearchFilter=(&(objectClass=posixGroup)(memberuid={0}))
    - ldapRealm.groupObjectClass=posixGroup
- Ldap账号结构
    - dc=hadoop,dc=apache,dc=org
        - ou=zeppelin: (phpLDAPadmin上面的Generic: Organisational Unit类型)
            - user1:(phpLDAPadmin上面的Generic: User Account类型)
            - user2: (phpLDAPadmin上面的Generic: User Account类型)
        - ou=groups: (Generic: Organisational Unit类型)
            - admin_group:(Generic: Posix Group类型)
            - ldap_group1:(Generic: Posix Group类型)
            - 创建group的后记得通过phpLDAPadmin上的	Add new attribute按钮添加memberUid属性并将需要添加到改组的用户名
            依次添加到这个属性下面，登陆zeppelin时这个group就可以映射成这些用户的角色了
            
------
#### 修改源码的方式
- [zeppelin集成openldap，以及admin用户设置](https://blog.csdn.net/woloqun/article/details/100561594): 这里面介绍了需要修改的地方
- 配置文件
```bash
[users]
#user3 = password4, role2

# Sample LDAP configuration, for user Authentication, currently tested for single Realm
[main]
ldapRealm=org.apache.zeppelin.realm.LdapRealm
ldapRealm.contextFactory.authenticationMechanism=simple
ldapRealm.contextFactory.url=ldap://ldapserver:389
ldapRealm.userDnTemplate=cn={0},ou=zeppelin,dc=domain,dc=com
ldapRealm.pagingSize = 200
ldapRealm.authorizationEnabled=true
ldapRealm.searchBase= dc=domain,dc=com
ldapRealm.userSearchBase = ou=zeppelin,dc=domain,dc=com
ldapRealm.groupSearchBase = ou=groups,dc=domain,dc=com
ldapRealm.groupObjectClass= posixGroup
ldapRealm.userLowerCase = true
ldapRealm.userSearchScope = subtree;
ldapRealm.groupSearchScope = subtree;
ldapRealm.contextFactory.systemUsername= cn=admin,dc=domain,dc=com
ldapRealm.contextFactory.systemPassword= adminpassword
ldapRealm.groupSearchEnableMatchingRuleInChain = true
## LDAP group到zeppelin shiro role的关系映射
ldapRealm.rolesByGroup = admin: admin, group1: role1
## 指定只有拥有admin或者role1角色的用户能登录
ldapRealm.allowedRolesForAuthentication = admin,role1
## 可参考https://www.w3cschool.cn/shiro/skex1if6.html进行Shiro权限字符串配置
#ldapRealm.permissionsByRole= user_role = *:ToDoItemsJdo:*:*, *:ToDoItem:*:*; admin_role = *


sessionManager = org.apache.shiro.web.session.mgt.DefaultWebSessionManager
cookie = org.apache.shiro.web.servlet.SimpleCookie
cookie.name = JSESSIONID
cookie.httpOnly = true
sessionManager.sessionIdCookie = $cookie
securityManager.sessionManager = $sessionManager
securityManager.sessionManager.globalSessionTimeout = 86400000
shiro.loginUrl = /api/login
securityManager.sessionManager = $sessionManager
securityManager.realms = $ldapRealm

[roles]
role1 = *
role2 = *
role3 = *
admin = *

[urls]
# This section is used for url-based security. For details see the shiro.ini documentation.
#
# You can secure interpreter, configuration and credential information by urls.
# Comment or uncomment the below urls that you want to hide:
# anon means the access is anonymous.
# authc means form based auth Security.
#
# IMPORTANT: Order matters: URL path expressions are evaluated against an incoming request
# in the order they are defined and the FIRST MATCH WINS.
#
# To allow anonymous access to all but the stated urls,
# uncomment the line second last line (/** = anon) and comment the last line (/** = authc)
#
/api/version = anon
/api/cluster/address = anon
# Allow all authenticated users to restart interpreters on a notebook page.
# Comment out the following line if you would like to authorize only admin users to restart interpreters.
/api/interpreter/setting/restart/** = authc
/api/interpreter/** = authc, roles[admin]
/api/notebook-repositories/** = authc, roles[admin]
/api/configurations/** = authc, roles[admin]
/api/credential/** = authc, roles[admin]
/api/admin/** = authc, roles[admin]
#/** = anon
/** = authc
```
- Ldap账号结构: 跟第一种方式一样
    - dc=domain,dc=com
        - ou=zeppelin: (phpLDAPadmin上面的Generic: Organisational Unit类型)
            - user1:(phpLDAPadmin上面的Generic: User Account类型)
            - user2: (phpLDAPadmin上面的Generic: User Account类型)
        - ou=groups: (Generic: Organisational Unit类型)
            - admin:(Generic: Posix Group类型)
            - group1:(Generic: Posix Group类型)
            - 创建group的后记得通过phpLDAPadmin上的	Add new attribute按钮添加memberUid属性并将需要添加到改组的用户名
            依次添加到这个属性下面，登陆zeppelin时这个group就可以映射成这些用户的角色了