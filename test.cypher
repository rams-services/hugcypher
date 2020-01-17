-- :name get-users :list :* :dp
MATCH (u:User)-[:Audit]->(n:User) where u <> n RETURN u, n limit 10

-- :name get-users-obj :obj :* 
MATCH (u:User)-[:Audit]->(n:User)
{% if only-active? %} WHERE n.active = TRUE AND u <> n AND n.username <> $username
{% else %} where u <> n {% endif %}
RETURN u, n limit 10

-- :name is-user-active? :bool :1
MATCH (u:User {username:$user-id}) where u.active = true return u

-- :name get-user :obj :1 :d
MATCH (u:User {username:$user-id}) RETURN u

-- :name get-user-client :list :1 :p
MATCH  (u:User {username:$user-id})-[:Has]->(c:Client) 
{% if only-active? %} WHERE u.active = false{% endif %} RETURN u, c

-- :name change-user-name :obj :1
-- :audit {:by {:node "User" :attribute "username" :param :by} :nodes [f] :message :msg}
MATCH (f:User {username:$user-id}) SET f.`newhere`=$info RETURN f 