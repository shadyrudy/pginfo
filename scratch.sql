
SELECT r.rolname
	,r.rolsuper
	,r.rolinherit
	,r.rolcreaterole
	,r.rolcreatedb
	,r.rolcanlogin
	,r.rolreplication
	,r.rolconnlimit
	,r.rolvaliduntil
	, ARRAY(SELECT b.rolname
    FROM pg_catalog.pg_auth_members m
    JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid) AS memberof
	, r.rolconfig
FROM pg_catalog.pg_roles r
where rolname not like 'pg_%'
ORDER BY 1;