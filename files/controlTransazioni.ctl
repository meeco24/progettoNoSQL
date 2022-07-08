LOAD DATA
INSERT INTO TABLE transazioni
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
id,
codice_transazione,
importo,
emittente,
beneficiario,
banca,
data
)
