import subprocess
import connection as c
import time

absolutePath = "/home/oracle/Documents/progettoDatabaseNoSQL/files/"

def createTables(entities):
    
    for x in entities:

        #preparazione dati
        nome = x
        attributi = entities[x][0]

        #creazione query
        q = """
            CREATE TABLE """ + nome + """ (
        """ + attributi + """
        )"""

        #esecuzioe query
        c.conn.cursor().execute(q)
        c.conn.commit()
        c.conn.cursor().close()

def dropTables(entities):

    for nome in entities:

        #preparazione query
        q = "DROP TABLE " + nome + " CASCADE CONSTRAINTS"

        #esecuzione query
        c.conn.cursor().execute(q)
        

    c.conn.commit()
    c.conn.cursor().close()

def loadData(entities, round):

        for x in entities:

                #preparazione dati
                controlFile = absolutePath + entities[x][2]

                if (len(entities[x][1]) > 1):
                        datasetCSV = absolutePath + entities[x][1][round]
                else:
                        datasetCSV = absolutePath + entities[x][1][0]

                subprocess.call(f"sqlldr userid={c.conStr} control={controlFile} data={datasetCSV}",  shell=True)

def clearCaches():

    cur = c.conn.cursor()
    cur.execute("alter system flush buffer_cache")
    cur.execute("alter system flush shared_pool")
    cur.close()

def executeAndRecord(query, cursor, titolo, dataset):
    #sostituire titolo file per avere i risultati dei diversi dataset su file distinti
    f = open(f"/home/oracle/Documents/progettoDatabaseNoSQL/files/oracleRisultati{dataset}.txt","a")
    f.write(f"{titolo}:\n")
    for i in range(31):

        before = time.time()
        cursor.execute(query)
        after = time.time()
        result = (after-before) * 1000 
    
        f.writelines(str(round(result,2))+"\n")
        
    f.write("\n\n\n")
    f.close()
    
    clearCaches()

def main():

    n_att = """
            id INT,
            nome VARCHAR(255),
            PRIMARY KEY(id)
    """
    
    b_att = """
            id INT,
            nome VARCHAR(255),
            sede INT,
            PRIMARY KEY (id),
            FOREIGN KEY (sede) REFERENCES nazioni(id) ON DELETE CASCADE
    """

    p_att = """
            id INT,
            nome VARCHAR(255),
            data_nascita VARCHAR(255),
            nazionalita INT,
            PRIMARY KEY (id),
            FOREIGN KEY (nazionalita) REFERENCES nazioni(id) ON DELETE CASCADE
    """

    a_att = """
            id INT,
            nome VARCHAR(255),
            data_fondazione VARCHAR(255),
            sede_legale INT,
            PRIMARY KEY (id),
            FOREIGN KEY (sede_legale) REFERENCES nazioni(id) ON DELETE CASCADE
    """

    t_att = """
            id INT,
            codice_transazione INT,
            importo FLOAT,
            emittente INT,
            beneficiario INT,
            banca INT,
            data VARCHAR(255),
            PRIMARY KEY (id),
            FOREIGN KEY (emittente) REFERENCES aziende(id) ON DELETE CASCADE,
            FOREIGN KEY (beneficiario) REFERENCES aziende(id) ON DELETE CASCADE,
            FOREIGN KEY (banca) REFERENCES banche(id) ON DELETE CASCADE
    """

    q_att = """
            azienda INT,
            azionista INT,
            quota INT,
            ubo INT,
            PRIMARY KEY (azienda, azionista, ubo),
            FOREIGN KEY (azienda) REFERENCES aziende(id) ON DELETE CASCADE
    """
    
    entities = {
        "nazioni" : [n_att, ["nazioni.csv"], "controlNazioni.ctl"],
        "banche" : [b_att, ["banche.csv"], "controlBanche.ctl"],
        "persone" : [p_att, ["persone2500.csv", "persone5000.csv", "persone7500.csv", "persone10000.csv", ], "controlPersone.ctl"],
        "aziende" : [a_att, ["aziende6250.csv, "+absolutePath+"aziende-6250.csv", "aziende12500.csv, "+absolutePath+"aziende-12500.csv", "aziende18750.csv, "+absolutePath+"aziende-18750.csv", "aziende25000.csv, "+absolutePath+"aziende-25000.csv"], "controlAziende.ctl"],
        "transazioni" : [t_att, ["transazioni3750.csv", "transazioni7500.csv", "transazioni11250.csv", "transazioni15000.csv"], "controlTransazioni.ctl"],
        "quotazioni" : [q_att, ["quotazioni-aziende6250.csv", "quotazioni-aziende12500.csv", "quotazioni-aziende18750.csv", "quotazioni-aziende25000.csv"], "controlQuotazioni.ctl"]
    }

    datasets = [0,1,2,3]

    query1 = "SELECT quotazioni.azienda, quotazioni.azionista, quotazioni.ubo FROM aziende join quotazioni on aziende.id = quotazioni.azienda where aziende.id = 0"
    query2 = "SELECT nome, 'persona' as tipo FROM persone WHERE nazionalita = 3 UNION SELECT nome, 'azienda' as tipo FROM aziende WHERE sede_legale = 3 UNION SELECT nome, 'banca' as tipo FROM banche WHERE sede = 3"
    query3 = "SELECT quotazioni.azionista FROM transazioni JOIN quotazioni ON transazioni.beneficiario = quotazioni.azienda where quotazioni.quota > 50 and transazioni.id = 150"
    query4 = "select banche.nome as banca, count(distinct aziende.nome) as aziende from transazioni join banche on transazioni.banca = banche.id join aziende on transazioni.emittente = aziende.id where transazioni.importo >= 100 group by (banche.nome) order by aziende desc"
    query5 = "select aziende.nome as nome, sum(transazioni.importo) as entrate from aziende join transazioni on aziende.id = transazioni.beneficiario group by aziende.nome order by entrate desc"

    queries = [query1, query2, query3, query4, query5]

    for dataset in datasets:

        createTables(entities)

        loadData(entities, dataset)

        n = 0
        for query in queries:
                n += 1
                cursor = c.conn.cursor()
                titolo = "Query" + str(n)
                executeAndRecord(query, cursor, titolo, dataset)
        
        dropTables(entities)



if __name__ == "__main__":
    main()