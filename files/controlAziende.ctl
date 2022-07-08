LOAD DATA
INSERT INTO TABLE aziende
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
id,
nome,
data_fondazione,
quotazioni FILLER char(300),
sede_legale
)
