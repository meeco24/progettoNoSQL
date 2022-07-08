from neo4j import GraphDatabase
import time
import dataLoading as load

def executeAndRecord(query, session, titolo, dataset):
    f = open(f"/home/meeco/Documenti/BD2Project/progettoDatabaseNoSQL/files/neo4jRisultati{dataset}.txt","a")
    f.write(f"{titolo}:\n")
    for i in range(31):
        before = time.time()
        session.run(query)
        after = time.time()
        result = (after-before) * 1000
        f.writelines(str(round(result,2))+"\n")
        
    f.write("\n\n\n")
    f.close()

    session.run("CALL db.clearQueryCaches()")

def main():

    uri = "neo4j://localhost:7687" #uri per connettersi al db
    driver = GraphDatabase.driver(uri, auth=("neo4j", "progetto123")) #credenziali di accesso al db

    session = driver.session()

    query1 = """
            MATCH (n)-[r:POSSIEDE]->(a:AZIENDA)
            WHERE a.id = 0
            RETURN n.nome as Azionisti
    """

    query2 = """
            MATCH (p:PERSONA)-[:NAZIONALITÀ]->(n:NAZIONE) 
            WHERE n.id = 3
            RETURN p.nome as nome, "persona" as tipo
            UNION
            MATCH (a:AZIENDA)-[:SEDE_LEGALE]->(n:NAZIONE) 
            WHERE n.id = 3
            RETURN a.nome as nome, "azienda" as tipo
            UNION
            MATCH (b:BANCA)-[:SEDE]->(n:NAZIONE) 
            WHERE n.id = 3
            RETURN b.nome as nome, "banca" as tipo     
    """

    query3 = """
            MATCH (a:AZIENDA)-[t:TRANSAZIONE_IN_USCITA]->(b:BANCA),
                    (x)-[p:POSSIEDE]->(c:AZIENDA)
            WHERE t.id = 0 and c.id = t.beneficiario and p.quota >= 50
            RETURN x.nome AS maggiori_azionisti
    """

    query4 = """
            MATCH (a:AZIENDA)-[t:TRANSAZIONE_IN_USCITA]->(b:BANCA)
            WHERE t.importo >= 100.0
            RETURN COUNT(DISTINCT t) AS transazioni, b.nome ORDER BY transazioni DESC  
    """

    query5 = """
            MATCH (b:BANCA)-[t:TRANSAZIONE_IN_ENTRATA]->(a:AZIENDA)
            RETURN a.nome AS azienda, SUM(t.importo) as entrate ORDER BY entrate DESC
    """

    datasets = [
        ["persone2500.csv", "aziende6250.csv", "aziende-6250.csv", "transazioni3750.csv"],
        ["persone5000.csv", "aziende12500.csv", "aziende-12500.csv", "transazioni7500.csv"],
        ["persone7500.csv", "aziende18750.csv", "aziende-18750.csv", "transazioni11250.csv"],
        ["persone10000.csv", "aziende25000.csv", "aziende-25000.csv", "transazioni15000.csv"],
        ]

    queries = [query1, query2, query3, query4, query5]

    for x in range(len(datasets)):

#CARICAMENTO DATI

            load.loadBanche(session)
            load.loadNazioni(session)
            load.loadPersone(session, datasets[x][0])
            load.loadAziende(session, datasets[x][1])
            load.loadAziendePersona(session, datasets[x][2])

#GENERAZIONE RELAZIONI
            load.sedeBanche(session)
            load.sedeAziende(session)
            load.nazionalita(session)

            load.quotaAzienda(session, datasets[x][1])
            load.quotaAziendaPersona(session, datasets[x][2])

            load.transazioniInUscita(session, datasets[x][3])
            load.transazioniInEntrata(session, datasets[x][3])

#ESECUZIONE QUERIES E RACCOLTA DEI TEMPI DI ESECUZIONE

            n = 0

            for query in queries:
                n +=1
                executeAndRecord(query, session, "Query"+ str(n), x)

#ELIMINAZIONE DI TUTTO IL CONTENUTO DEL DB

            session.run("MATCH (n) DETACH DELETE n")
    

if __name__ == "__main__":
    main()

