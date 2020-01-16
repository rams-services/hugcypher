-- :name get-users :list :* :dp
MATCH (u:User)-[:Audit]->(n:User) where u <> n RETURN u, n limit 1

-- :name get-users-obj :obj :* :dp
MATCH (u:User)-[:Audit]->(n:User) where u <> n RETURN u, n limit 1

-- :name get-user :obj :1 :d
MATCH (u:User {username:$user-id}) RETURN u

-- :name get-user-client :list :1 :p
MATCH  (u:User {username:$user-id})-[:Has]->(c:Client) RETURN u, c

-- :name change-user-name :obj :1
-- :audit {:by {:node "User" :attribute "username" :param :by} :nodes [f] :message "a message"}
MATCH (f:User {username:$user-id}) SET f.`newhere`=$info RETURN f 