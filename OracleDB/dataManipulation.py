import csv
import json

def main():
    
    filesPath = "/home/oracle/Documents/progettoDatabaseNoSQL/files/"
    files = ["aziende6250.csv", "aziende-6250.csv", "aziende12500.csv", "aziende-12500.csv", "aziende18750.csv", "aziende-18750.csv", "aziende25000.csv", "aziende-25000.csv"]

    for x in files:

        if "-" in x:
            ubo = 1
            suffisso = x.replace("-","")
        else:
            ubo = 0
            suffisso = x

        with open(filesPath+x) as file:
            csv_reader = csv.reader(file)
            next(csv_reader)

            diz = {}

            for line in csv_reader:
                diz[line[0]] = json.loads((line[3].replace("'",'"')))

        with open(filesPath+"quotazioni-"+suffisso, "a") as new_file:
            csv_writer = csv.writer(new_file)

            csv_writer.writerow(["azienda","azionista","quota","ubo"])

            for key,value in diz.items():
                for azionista in value:
                    csv_writer.writerow([key, azionista["azionista"], azionista["quota"], ubo, ""])
        

if __name__ == "__main__":
    main()


