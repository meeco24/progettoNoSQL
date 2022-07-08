LOAD DATA
APPEND
INTO TABLE quotazioni
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
(
azienda,
azionista,
quota,
ubo,
niente FILLER CHAR(1)
)
