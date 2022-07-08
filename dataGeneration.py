from datetime import datetime
from faker import Faker
import random
import pandas

Faker.seed(1234)

fake = Faker()

# -------------------------- DEFINIZIONE FUNZIONI --------------------------

#creazione dati aziende
def creaAziende(nazioni, n_aziende):
    
    aziende = []
    
    for i in range(n_aziende):
        aziende.append({
            'id': i,
            'nome': fake.company(),
            'data_fondazione': fake.date_between_dates(date_start=datetime(1960,1,1), date_end=datetime(1999,12,12)),
            'sede': nazioni[random.randint(0, len(nazioni)-1)]["id"],
            'quotazioni': {}
        })

    return aziende

#creazione dati banche
def creaBanche(nazioni, n_banche):
    
    banche = []
    
    for i in range(n_banche):
        banche.append({
            'id': i,
            'nome': fake.company(),
            'sede': nazioni[random.randint(0, len(nazioni)-1)]["id"],
        })
    
    return banche

#creazione dati nazioni
def creaNazioni(n_nazioni):
    
    nazioni = []
    
    for i in range(n_nazioni):
        nazioni.append({
            'id': i,
            'nome': fake.country()
        })

    return nazioni

#creazione dati persone
def creaPersone(nazioni, n_persone):
    
    persone = []
    
    for i in range(n_persone):
        persone.append({
            'id':i,
            'nome':fake.name(),
            'data_nascita':fake.date_between_dates(date_start=datetime(1932,1,1), date_end=datetime(2004,12,12)),
            'nazionalita': nazioni[random.randint(0, len(nazioni)-1)]["id"]
        })
    
    return persone

#creazione dati transazioni
def creaTransazioni (aziende, banche, n_trans):
    
    transazioni = []
    
    for t in range(n_trans):
        
        emittente = aziende[random.randint(0, len(aziende)-1)]["id"]
        beneficiario = aziende[random.randint(0, len(aziende)-1)]["id"]
        
        while (beneficiario == emittente):
            beneficiario = aziende[random.randint(0, len(aziende)-1)]["id"]
        
        transazioni.append({
            'id': t,
            'codice_transazione': random.randint(1000, 1000000),
            'importo': round(random.uniform(1000, 1000000), 2),
            'emittente': emittente,
            'beneficiario': beneficiario,
            'banca': banche[random.randint(0, len(banche)-1)]["id"],
            'data': fake.date_between_dates(date_start=datetime(2000,1,1), date_end=datetime(2022,6,6))
        })

    return transazioni

#funzione per assegnare randomicamente le quote aziendali
def assegnaQuote (gruppoA, gruppoB):
    for a in gruppoA:
        
        a["quotazioni"] = []
        
        bCopy = gruppoB.copy()
        quotaMax = 100
        
        while(quotaMax>0):
            quota = random.randint(1,100)

        #condizione uscita while
            if(quota>quotaMax):
                quota=quotaMax
                quotaMax=0
        #assegnazione randomica di un azionista dal gruppoB
            azionista = bCopy.pop(random.randint(0,len(bCopy)-1))
        
            while(azionista["id"] == a["id"]):
                azionista = bCopy.pop(random.randint(0,len(bCopy)-1))

        #se dopo il pop la lista è vuota 
        #assegno tutta la quota restante all'ultima azienda randomica del gruppoB
            if(len(bCopy) == 0):
                a["quotazioni"].append({"azionista":azionista["id"], "quota":quotaMax})
                break
            
        #aggiungo a 'quotazioni' azienda:quota
            a["quotazioni"].append({"azionista":azionista["id"], "quota":quota})
        
        #decremento quotaMax, condizione di uscita del while
            quotaMax -= quota

#creazione CSV
def creaCSV(dati, attributi, titolo):
    path = "/home/meeco/Documenti/BD2Project/progettoDatabaseNoSQL/files/" + titolo + ".csv"
    dataFrame = pandas.DataFrame(dati, columns=attributi)
    dataFrame.to_csv(path_or_buf=path, index=False)

# -------------------------- MAIN --------------------------

def main():

# --- NAZIONI ---

    nazioni = creaNazioni(104)
    nazioni_att = ['id','nome']
    
    creaCSV(nazioni, nazioni_att, 'nazioni')


# --- BANCHE ---

    banche = creaBanche(nazioni, 120)
    banche_att = ['id','nome','sede']
    
    creaCSV(banche, banche_att, 'banche')

    
# --- PERSONE, AZIENDE E TRANSAZIONI ---

    datasetsP = [10000, 7500, 5000, 2500]
    persone_att = ['id','nome','data_nascita', 'nazionalita']

    datasetsA = [25000, 18750, 12500, 6250]
    aziende_att = ['id','nome','data_fondazione','quotazioni', 'sede']

    datasetsT = [15000, 11250, 7500, 3750]
    transazioni_att = ['id', 'codice_transazione', 'importo', 'emittente', 'beneficiario', 'banca', 'data']

    for x in range(4):

        #PERSONE

        n_persone = datasetsP[x] #numero di persone per creare i diversi dataset
        persone = creaPersone(nazioni, n_persone)
    
        creaCSV(persone, persone_att, 'persone' + str(n_persone))

        print("persone" + str(x) + "finito")
        
        #AZIENDE

        n_aziende = datasetsA[x] #numero di aziende per creare i diversi dataset
        aziende = creaAziende(nazioni, n_aziende)

        trentaPercento = (len(aziende)*30//100)-1
        settantaPercento = (len(aziende)*70//100)
        
        gruppoA = aziende[:trentaPercento] #30% 
        gruppoB = aziende[trentaPercento:settantaPercento] #40%
        gruppoC = aziende[settantaPercento:] #30%
        
        assegnaQuote(gruppoA, gruppoB)
        assegnaQuote(gruppoB, gruppoC)
        assegnaQuote(gruppoC, persone)
        
        creaCSV(gruppoA+gruppoB, aziende_att, 'aziende' + str(n_aziende)) #aziende possedute da altre aziende
        creaCSV(gruppoC, aziende_att, 'aziende-' + str(n_aziende)) #aziende possedute da persone

        print("aziende" + str(x) + "finito")

#       TRANSAZIONI

        n_transazioni = datasetsT[x] #numero di transazioni per creare i diversi dataset
        transazioni = creaTransazioni(aziende, banche, n_transazioni)
        
        creaCSV(transazioni, transazioni_att, 'transazioni' + str(n_transazioni))

        print("transazioni" + str(x) + "finito")

        
    
# -------------------------- ESECUZIONE MAIN --------------------------
if __name__ == "__main__":
    main()