#FILE CONTENENTE LE FUNZIONI PER CARICARE I DATI SUL DB TRAMITE CSV E PER GENERARE LE RELAZIONI

#query per caricare il csv con i dati sulle PERSONE sul database
def loadPersone(session, file):
        session.run(f"""
        call apoc.load.csv('file:///{file}') """ + """
        yield map as row
        merge (p:PERSONA {nome:row.nome, id:toInteger(row.id), data_nascita:row.data_nascita, nazionalita:row.nazionalita})
                """)

#query per caricare il csv con i dati sulle AZIENDE sul database
def loadAziende(session, file):
        session.run(f"""
        call apoc.load.csv('file:///{file}') """ + """
        yield map as row
        merge (a:AZIENDA {nome:row.nome, id:toInteger(row.id), data_fondazione:row.data_fondazione, sede:row.sede})
                """)

#query per caricare il csv con i dati sulle AZIENDE posseduto da persone sul database
def loadAziendePersona(session, file):
        session.run(f"""
        call apoc.load.csv('file:///{file}') """ + """
        yield map as row
        merge (a:AZIENDA {nome:row.nome, id:toInteger(row.id), data_fondazione:row.data_fondazione, sede:row.sede})
                """)

#query per caricare il csv con i dati sulle BANCHE sul database
def loadBanche(session):
        session.run("""
        call apoc.load.csv('file:///banche.csv')
        yield map as row
        merge (b:BANCA {id:toInteger(row.id), nome:row.nome, sede:row.sede})
        """)

#query per caricare il csv con i dati sulle NAZIONI sul database
def loadNazioni(session): 
        session.run("""
        call apoc.load.csv('file:///nazioni.csv')
        yield map as row
        merge (n:NAZIONE {id:toInteger(row.id), nome:row.nome})
                """)

#GENERAZIONE RELAZIONI

#query creazione relazione sede banche
def sedeBanche(session):
        session.run('''
        MATCH (b:BANCA), (n:NAZIONE)
        WHERE toInteger(b.sede) = toInteger(n.id)
        CREATE (b)-[:SEDE]->(n)
        ''')

#query creazione relazione sede aziende
def sedeAziende(session):
        session.run('''
        MATCH (a:AZIENDA), (n:NAZIONE)
        WHERE toInteger(a.sede) = toInteger(n.id)
        CREATE (a)-[:SEDE_LEGALE]->(n)
        ''')

#query creazione relazione residenza persone
def nazionalita(session):
        session.run('''
        MATCH (a:PERSONA), (n:NAZIONE)
        WHERE toInteger(a.nazionalita) = toInteger(n.id)
        CREATE (a)-[:NAZIONALITÀ]->(n)
        ''')

#query assegnazione quote aziende
def quotaAzienda(session, file):
        session.run(f'''
        call apoc.load.csv('file:///{file}') ''' + '''
        yield map as row
        UNWIND apoc.convert.fromJsonList(row.quotazioni) as r
        match (n:AZIENDA),(m:AZIENDA)
        WHERE toInteger(n.id) = toInteger(row.id) AND toInteger(m.id) = toInteger(r.azionista)
        create (n)<-[:POSSIEDE {quota:r.quota}]-(m)
        ''')

#query assegnazione quote persone
def quotaAziendaPersona(session, file):
        session.run(f'''
        call apoc.load.csv('file:///{file}') ''' + '''
        yield map as row
        UNWIND apoc.convert.fromJsonList(row.quotazioni) as r
        match (n:AZIENDA),(m:PERSONA)
        WHERE toInteger(n.id) = toInteger(row.id) AND toInteger(m.id) = toInteger(r.azionista)
        create (n)<-[:POSSIEDE {quota:r.quota}]-(m)
        ''')

#query per creare le transazioni in uscita azienda->banca
def transazioniInUscita(session, file):
        session.run(f"""
        call apoc.load.csv('file:///{file}') """ + """
        yield map as row
        match (a:AZIENDA), (b:BANCA)
        where a.id = toInteger(row.emittente) and b.id = toInteger(row.banca)
        merge (a)-[t:TRANSAZIONE_IN_USCITA {id:toInteger(row.id), codice_transazione:row.codice_transazione, importo:toFloat(row.importo), beneficiario:toInteger(row.beneficiario) ,banca:toInteger(row.banca), data:row.data}]->(b)
        """)

#query per creare le transazioni in entrata banca->azienda
def transazioniInEntrata(session, file):
        session.run(f"""
        call apoc.load.csv('file:///{file}') """ + """
        yield map as row
        match (a:AZIENDA), (b:BANCA)
        where a.id = toInteger(row.beneficiario) and b.id = toInteger(row.banca)
        merge (b)-[t:TRANSAZIONE_IN_ENTRATA {id:toInteger(row.id), codice_transazione:row.codice_transazione, importo:toFloat(row.importo), beneficiario:toInteger(row.beneficiario) ,banca:toInteger(row.banca), data:row.data}]->(a)
        """)
