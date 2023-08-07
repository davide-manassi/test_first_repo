import numpy as np
import pandas as pd
from openpyxl import *
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.chart import BarChart, Series, Reference
from datetime import datetime, timedelta

# --------------------------------------------


def calls_chetariffa_2(lista_db):
    db_tot = pd.DataFrame()
    for nome_db in lista_db:
        db = pd.read_csv(nome_db + ".csv", sep=';', low_memory=False, usecols=["EsitoTelefonico", "CallStarDate", "Id",
                                                                               "NomeLista", "AgenteTelefonico",
                                                                               "AppuntamentoPersonale",
                                                                               "CallSecondDuration", "Index",
                                                                               "CallEndDate", "PhoneNumber"])
        db_tot = pd.concat([db_tot, db]).reset_index(drop=True)

    for i in range(0, len(db_tot)):
        try:
            db_tot.at[i, 'Id'] = int(db_tot.at[i, 'Id'])
        except ValueError:
            db_tot.at[i, 'Id'] = db_tot.at[i, 'Id']

    db_tot['EsitoTelefonico_calls'] = db_tot['EsitoTelefonico']
    db_tot.drop('EsitoTelefonico', axis=1)

    return db_tot


def lead_chetariffa(lista_db):
    db_tot = pd.DataFrame()
    for nome_db in lista_db:
        db = pd.read_csv(nome_db + ".csv", sep=';', low_memory=False, usecols=["TotaleTentativi", "DataUltimaChiamata",
                                                                               "Id", "NomeLista", "fonte", "DataImport",
                                                                               "UltimoAgente", "StatoContatto", "CreationDate",
                                                                               "EsitoTelefonico", "data_ricezione_lead_out",
                                                                               "NomeLista", "Target"])
        db['TotaleTentativi'] = db['TotaleTentativi'].astype('Int64')
        db_tot = pd.concat([db_tot, db]).reset_index(drop=True)

    for i in range(0, len(db_tot)):
        try:
            db_tot.at[i, 'Id'] = int(db_tot.at[i, 'Id'])
        except ValueError:
            db_tot.at[i, 'Id'] = db_tot.at[i, 'Id']

    db_tot['EsitoTelefonico_lead'] = db_tot['EsitoTelefonico']
    return db_tot


def filtra_periodo(db, campo, data_inizio, data_fine):
    db[campo] = pd.to_datetime(db[campo], format="%d/%m/%Y %H:%M", exact=False)
    date_start = pd.to_datetime(data_inizio, format="%d/%m/%Y")
    date_end = pd.to_datetime(data_fine, format="%d/%m/%Y")
    filt_periodo = (db[campo] >= date_start) & (db[campo] < date_end + timedelta(days=1))
    db = pd.DataFrame(db[filt_periodo]).reset_index(drop=True)

    return db

def filtra_per_giorno(db, campo, data_inizio, data_fine):
    db[campo] = pd.to_datetime(db[campo], format="%d/%m/%Y", exact=False)
    date_start = pd.to_datetime(data_inizio, format="%d/%m/%Y")
    date_end = pd.to_datetime(data_fine, format="%d/%m/%Y")
    filt_periodo = (db[campo] >= date_start) & (db[campo] < date_end + timedelta(days=1))
    db = pd.DataFrame(db[filt_periodo]).reset_index(drop=True)

    return db

NA_Campo = [None, np.nan, np.NaN, np.NAN, ""]


def pulisci_da_campi_vuoti(db, campo):
    for i in range(0, len(db)):
        if db.at[i, campo] in NA_Campo:
            db = db.drop([i])

    return db.reset_index(drop=True)


def seleziona_stato(db, lista_stati):
    filt_stati = db['StatoContatto'].isin(lista_stati)

    return db[filt_stati].reset_index(drop=True)


def db_a_periodi(db, campo, mese, anno):
    db['mese_campo'] = pd.to_datetime(db[campo], format="%d/%m/%Y", exact=False).dt.strftime("%m").astype('Int64')
    db['anno_campo'] = pd.to_datetime(db[campo], format="%d/%m/%Y", exact=False).dt.strftime("%Y").astype('Int64')

    filt_anno = (db['anno_campo'] == anno)
    db = pd.DataFrame(db[filt_anno]).reset_index(drop=True)

    db_mensile = pd.DataFrame()
    db_trimestrale = pd.DataFrame()
    db_semestrale = pd.DataFrame()
    db_annuale = pd.DataFrame()

    if mese == "Gennaio":
        db_mensile = pd.DataFrame(db[db['mese_campo'] == 1]).reset_index(drop=True)
        db_trimestrale = db_mensile
        db_semestrale = db_mensile
        db_annuale = db_mensile
    if mese == "Febbraio":
        db_mensile = pd.DataFrame(db[db['mese_campo'] == 2]).reset_index(drop=True)
        db_trimestrale = pd.DataFrame(db[db['mese_campo'].isin([1, 2])]).reset_index(drop=True)
        db_semestrale = db_trimestrale
        db_annuale = db_trimestrale
    if mese == "Marzo":
        db_mensile = pd.DataFrame(db[db['mese_campo'] == 3]).reset_index(drop=True)
        db_trimestrale = pd.DataFrame(db[db['mese_campo'].isin([1, 2, 3])]).reset_index(drop=True)
        db_semestrale = db_trimestrale
        db_annuale = db_trimestrale
    if mese == "Aprile":
        db_mensile = pd.DataFrame(db[db['mese_campo'] == 4]).reset_index(drop=True)
        db_trimestrale = db_mensile
        db_semestrale = pd.DataFrame(db[db['mese_campo'].isin([1, 2, 3, 4])]).reset_index(drop=True)
        db_annuale = db_semestrale
    if mese == "Maggio":
        db_mensile = pd.DataFrame(db[db['mese_campo'] == 5]).reset_index(drop=True)
        db_trimestrale = pd.DataFrame(db[db['mese_campo'].isin([4, 5])]).reset_index(drop=True)
        db_semestrale = pd.DataFrame(db[db['mese_campo'].isin([1, 2, 3, 4, 5])]).reset_index(drop=True)
        db_annuale = db_semestrale
    if mese == "Giugno":
        db_mensile = pd.DataFrame(db[db['mese_campo'] == 6]).reset_index(drop=True)
        db_trimestrale = pd.DataFrame(db[db['mese_campo'].isin([4, 5, 6])]).reset_index(drop=True)
        db_semestrale = pd.DataFrame(db[db['mese_campo'].isin([1, 2, 3, 4, 5, 6])]).reset_index(drop=True)
        db_annuale = db_semestrale
    if mese == "Luglio":
        db_mensile = pd.DataFrame(db[db['mese_campo'] == 7]).reset_index(drop=True)
        db_trimestrale = db_mensile
        db_semestrale = db_mensile
        db_annuale = pd.DataFrame(db[db['mese_campo'].isin([1, 2, 3, 4, 5, 6, 7])]).reset_index(drop=True)
    if mese == "Agosto":
        db_mensile = pd.DataFrame(db[db['mese_campo'] == 8]).reset_index(drop=True)
        db_trimestrale = pd.DataFrame(db[db['mese_campo'].isin([7, 8])]).reset_index(drop=True)
        db_semestrale = db_trimestrale
        db_annuale = pd.DataFrame(db[db['mese_campo'].isin([1, 2, 3, 4, 5, 6, 7, 8])]).reset_index(drop=True)
    if mese == "Settembre":
        db_mensile = pd.DataFrame(db[db['mese_campo'] == 9]).reset_index(drop=True)
        db_trimestrale = pd.DataFrame(db[db['mese_campo'].isin([7, 8, 9])]).reset_index(drop=True)
        db_semestrale = db_trimestrale
        db_annuale = pd.DataFrame(db[db['mese_campo'].isin([1, 2, 3, 4, 5, 6, 7, 8, 9])]).reset_index(drop=True)
    if mese == "Ottobre":
        db_mensile = pd.DataFrame(db[db['mese_campo'] == 10]).reset_index(drop=True)
        db_trimestrale = db_mensile
        db_semestrale = pd.DataFrame(db[db['mese_campo'].isin([7, 8, 9, 10])]).reset_index(drop=True)
        db_annuale = pd.DataFrame(db[db['mese_campo'].isin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])]).reset_index(drop=True)
    if mese == "Novembre":
        db_mensile = pd.DataFrame(db[db['mese_campo'] == 11]).reset_index(drop=True)
        db_trimestrale = pd.DataFrame(db[db['mese_campo'].isin([10, 11])]).reset_index(drop=True)
        db_semestrale = pd.DataFrame(db[db['mese_campo'].isin([7, 8, 9, 10, 11])]).reset_index(drop=True)
        db_annuale = pd.DataFrame(db[db['mese_campo'].isin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])]).reset_index(drop=True)
    if mese == "Novembre":
        db_mensile = pd.DataFrame(db[db['mese_campo'] == 12]).reset_index(drop=True)
        db_trimestrale = pd.DataFrame(db[db['mese_campo'].isin([10, 11, 12])]).reset_index(drop=True)
        db_semestrale = pd.DataFrame(db[db['mese_campo'].isin([7, 8, 9, 10, 11, 12])]).reset_index(drop=True)
        db_annuale = pd.DataFrame(db[db['mese_campo'].isin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])]).reset_index(drop=True)

    db_mensile = db_mensile.drop(['mese_campo', 'anno_campo'], axis=1)
    db_trimestrale = db_trimestrale.drop(['mese_campo', 'anno_campo'], axis=1)
    db_semestrale = db_semestrale.drop(['mese_campo', 'anno_campo'], axis=1)
    db_annuale = db_annuale.drop(['mese_campo', 'anno_campo'], axis=1)

    return [db_mensile, db_trimestrale, db_semestrale, db_annuale]


def calls_stock(lista_db):
    db_tot = pd.DataFrame()
    for nome_db in lista_db:
        db = pd.read_csv(nome_db + ".csv", sep=';', low_memory=False, usecols=["EsitoTelefonico", "CallStarDate", "Controparte",
                                                                               "Nome_Delivery", "AgenteTelefonico",
                                                                               "CallSecondDuration", "Index",
                                                                               "CallEndDate", "PhoneNumber", "Fascia", "EsitoContatto"])
        db_tot = pd.concat([db_tot, db]).reset_index(drop=True)

    for i in range(0, len(db_tot)):
        try:
            db_tot.at[i, 'Controparte'] = int(db_tot.at[i, 'Controparte'])
        except ValueError:
            db_tot.at[i, 'Controparte'] = db_tot.at[i, 'Controparte']

    db_tot['EsitoTelefonico_calls'] = db_tot['EsitoTelefonico']
    db_tot.drop('EsitoTelefonico', axis=1)

    return db_tot


def calls_caa(lista_db):
    db_tot = pd.DataFrame()
    for nome_db in lista_db:
        db = pd.read_csv(nome_db + ".csv", sep=';', low_memory=False, usecols=["EsitoTelefonico", "CallStarDate", "Codice_Cliente",
                                                                               "Fascia", "AgenteTelefonico",
                                                                               "CallSecondDuration", "Index",
                                                                               "CallEndDate", "PhoneNumber", "TipoCliente", "EsitoContatto"])
        db_tot = pd.concat([db_tot, db]).reset_index(drop=True)

    for i in range(0, len(db_tot)):
        try:
            db_tot.at[i, 'Codice_Cliente'] = int(db_tot.at[i, 'Codice_Cliente'])
        except ValueError:
            db_tot.at[i, 'Codice_Cliente'] = db_tot.at[i, 'Codice_Cliente']

    db_tot['EsitoTelefonico_calls'] = db_tot['EsitoTelefonico']
    db_tot.drop('EsitoTelefonico', axis=1)

    return db_tot


def calls_santander(lista_db):
    db_tot = pd.DataFrame()
    for nome_db in lista_db:
        db = pd.read_csv(nome_db + ".csv", sep=';', low_memory=False, usecols=["EsitoTelefonico", "CallStarDate", "CodiceCliente",
                                                                               "TipoProdotto", "AgenteTelefonico", "AppuntamentoPersonale",
                                                                               "CallSecondDuration", "Index",
                                                                               "CallEndDate", "PhoneNumber", "EsitoContatto"])
        db_tot = pd.concat([db_tot, db]).reset_index(drop=True)

    for i in range(0, len(db_tot)):
        try:
            db_tot.at[i, 'CodiceCliente'] = int(db_tot.at[i, 'CodiceCliente'])
        except ValueError:
            db_tot.at[i, 'CodiceCliente'] = db_tot.at[i, 'CodiceCliente']

    db_tot['EsitoTelefonico_calls'] = db_tot['EsitoTelefonico']
    db_tot.drop('EsitoTelefonico', axis=1)

    return db_tot

def calls_giornaliera(lista_db):
    db_tot = pd.DataFrame()
    for nome_db in lista_db:
        db = pd.read_csv(nome_db + ".csv", sep=';', low_memory=False, usecols=["EsitoTelefonico", "CallStarDate", "Controparte",
                                                                               "Nome_Delivery", "AgenteTelefonico",
                                                                               "CallSecondDuration", "Index",
                                                                               "CallEndDate", "PhoneNumber", "EsitoContatto"])
        db_tot = pd.concat([db_tot, db]).reset_index(drop=True)

    for i in range(0, len(db_tot)):
        try:
            db_tot.at[i, 'Controparte'] = int(db_tot.at[i, 'Controparte'])
        except ValueError:
            db_tot.at[i, 'Controparte'] = db_tot.at[i, 'Controparte']

    db_tot['EsitoTelefonico_calls'] = db_tot['EsitoTelefonico']
    db_tot.drop('EsitoTelefonico', axis=1)

    return db_tot


def calls_movenzia(lista_db):
    db_tot = pd.DataFrame()
    for nome_db in lista_db:
        db = pd.read_csv(nome_db + ".csv", sep=';', low_memory=False, usecols=["EsitoTelefonico", "CallStarDate", "PhoneNumber",
                                                                               "NomeLista", "AgenteTelefonico",
                                                                               "CallSecondDuration", "Index",
                                                                               "CallEndDate", "EsitoContatto", "AppuntamentoPersonale", "OrderId"])
        db_tot = pd.concat([db_tot, db]).reset_index(drop=True)

    for i in range(0, len(db_tot)):
        try:
            db_tot.at[i, 'PhoneNumber'] = int(db_tot.at[i, 'PhoneNumber'])
        except ValueError:
            db_tot.at[i, 'PhoneNumber'] = db_tot.at[i, 'PhoneNumber']

    db_tot['EsitoTelefonico_calls'] = db_tot['EsitoTelefonico']
    db_tot.drop('EsitoTelefonico', axis=1)

    return db_tot
