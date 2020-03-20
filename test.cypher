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
MATCH (f:User {username:$user-id}) SET f.`username`=$info RETURN f

-- :name get-user-activity :list :*
MATCH (u:User {id:$user-id})-[r:Audit]->(q) RETURN r ORDER BY r.`created-on` desc

-- :name create-user :list :1
-- :audit {:by :by :message :action :nodes [u]}
CREATE (u:User {id:$id, `first-name`: $first-name, `last-name`: $last-name, `active?`: $active?, password: $password, `joined-on`: $now}) RETURN u

-- :name get-users :list :1
MATCH (u:User) OPTIONAL MATCH (u)-[:HasRole]->(r:Role) RETURN distinct u.id as id, u.`first-name` as `first-name`, u.`last-name` as `last-name`, u.`active?` as `active?`, u.`joined-on` as `joined-on`, collect({id: r.id, name: r.name}) as roles

-- :name get-user-with-credentials :list :1
MATCH (u:User) OPTIONAL MATCH (u)-[:HasRole]->(r:Role) RETURN u, collect({id: r.id, name: r.name}) as roles


-- :name give-user-module :list :1
-- :audit {:nodes [u, m, p]}
MATCH (u:User {id:$user-id})
MATCH (m:Module {id:$module-id})
MATCH (p:Permission {id:$permission})
MERGE (u)-[{{permission}}]->(m) return u, m, p

-- :name collect-data :list :*
MATCH (c:Constituents)-[:Contains]-(f) return c, collect(f) as collection
LIMIT 10

