import numpy as np
import pandas as pd
from openpyxl import *
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.chart import BarChart, Series, Reference
from datetime import datetime, timedelta
import sys
import mysql
import mysql.connector
from sqlalchemy import *
from sqlalchemy.engine import *
from sqlalchemy.sql import *
from urllib.parse import quote_plus
import sqlalchemy
from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
from time import sleep

engine = create_engine('mysql+mysqldb://root:%s@localhost/Wasabi' % quote_plus("J779pB76JYJ@"))
contacts = sqlalchemy.Table("contacts", sqlalchemy.MetaData(), autoload_with=engine)
ore = sqlalchemy.Table("sessions", sqlalchemy.MetaData(), autoload_with=engine)
operators = sqlalchemy.Table("operators", sqlalchemy.MetaData(), autoload_with=engine)
calls = sqlalchemy.Table("calls", sqlalchemy.MetaData(), autoload_with=engine)
CHUBB_FACILE = sqlalchemy.Table("CHUBB_FACILE", sqlalchemy.MetaData(), autoload_with=engine)


def bonifica_sessioni(db_sessioni):
    db_bonificato = pd.DataFrame(columns=['SessionLength', 'PayableTotal', 'Payable'])

    db_sessioni['StartDate'] = pd.to_datetime(db_sessioni['StartDate'], format="%Y-%m-%d %H:%M:%S.%f")
    db_sessioni['EndDate'] = pd.to_datetime(db_sessioni['EndDate'], format="%Y-%m-%d %H:%M:%S.%f")
    db_sessioni = db_sessioni.sort_values(by=['username', 'StartDate'])
    db_sessioni['WorkDay'] = db_sessioni['StartDate'].dt.strftime("%Y-%m-%d")
    db_sessioni['SessionLength'] = (db_sessioni['EndDate'] - db_sessioni['StartDate']) / timedelta(seconds=1)

    db_unico = pd.DataFrame(db_sessioni[['username', 'WorkDay']].drop_duplicates(subset=['username', 'WorkDay'])).reset_index(drop=True)

    for i in range(0, len(db_unico)):
        filt_op_giorno = (db_sessioni['username'] == db_unico.at[i, 'username']) & (db_sessioni['WorkDay'] == db_unico.at[i, 'WorkDay'])

        db_sessioni_daily_op = db_sessioni[filt_op_giorno].reset_index(drop=True)

        payable_tamporary = 0

        for j in range(0, len(db_sessioni_daily_op)):
            if j == 0:
                if db_sessioni_daily_op.at[j, 'Name'] in ["Pausa Non Retribuita"]:
                    db_sessioni_daily_op.at[j, 'PayableTotal'] = 0
                    db_sessioni_daily_op.at[j, 'Payable'] = 0
                    payable_tamporary = 0
                elif db_sessioni_daily_op.at[j, 'Name'] in ["Lavoro", "Pausa Formazione", "Pausa Staff NON UTILIZZARE"]:
                    db_sessioni_daily_op.at[j, 'PayableTotal'] = db_sessioni_daily_op.at[j, 'SessionLength']
                    db_sessioni_daily_op.at[j, 'Payable'] = db_sessioni_daily_op.at[j, 'SessionLength']
                    payable_tamporary += db_sessioni_daily_op.at[j, 'SessionLength']
                elif db_sessioni_daily_op.at[j, 'Name'] == "Pausa Legge 81":
                    if payable_tamporary >= 7200 and db_sessioni_daily_op.at[j, 'SessionLength'] <= 900:
                        payable_tamporary = 0
                        db_sessioni_daily_op.at[j, 'PayableTotal'] = db_sessioni_daily_op.at[j - 1, 'PayableTotal'] + db_sessioni_daily_op.at[j, 'SessionLength']
                        db_sessioni_daily_op.at[j, 'Payable'] = db_sessioni_daily_op.at[j, 'SessionLength']
                    elif payable_tamporary >= 7200 and db_sessioni_daily_op.at[j, 'SessionLength'] > 900:
                        payable_tamporary = 0
                        db_sessioni_daily_op.at[j, 'PayableTotal'] = db_sessioni_daily_op.at[j - 1, 'PayableTotal'] + 900
                        db_sessioni_daily_op.at[j, 'Payable'] = 900
                    elif payable_tamporary < 7200:
                        payable_tamporary = 0
                        db_sessioni_daily_op.at[j, 'PayableTotal'] = db_sessioni_daily_op.at[j - 1, 'PayableTotal']
                        db_sessioni_daily_op.at[j, 'Payable'] = 0

            else:
                if db_sessioni_daily_op.at[j, 'Name'] in ["Pausa Non Retribuita"]:
                    db_sessioni_daily_op.at[j, 'PayableTotal'] = db_sessioni_daily_op.at[j - 1, 'PayableTotal']
                    db_sessioni_daily_op.at[j, 'Payable'] = 0
                    payable_tamporary = 0
                elif db_sessioni_daily_op.at[j, 'Name'] in ["Lavoro", "Pausa Formazione", "Pausa Staff NON UTILIZZARE"]:
                    db_sessioni_daily_op.at[j, 'PayableTotal'] = db_sessioni_daily_op.at[j - 1, 'PayableTotal'] + db_sessioni_daily_op.at[j, 'SessionLength']
                    db_sessioni_daily_op.at[j, 'Payable'] = db_sessioni_daily_op.at[j, 'SessionLength']
                    payable_tamporary += db_sessioni_daily_op.at[j, 'SessionLength']
                elif db_sessioni_daily_op.at[j, 'Name'] == "Pausa Legge 81":
                    if payable_tamporary >= 7200 and db_sessioni_daily_op.at[j, 'SessionLength'] <= 900:
                        payable_tamporary = 0
                        db_sessioni_daily_op.at[j, 'PayableTotal'] = db_sessioni_daily_op.at[j - 1, 'PayableTotal'] + db_sessioni_daily_op.at[j, 'SessionLength']
                        db_sessioni_daily_op.at[j, 'Payable'] = db_sessioni_daily_op.at[j, 'SessionLength']
                    elif payable_tamporary >= 7200 and db_sessioni_daily_op.at[j, 'SessionLength'] > 900:
                        payable_tamporary = 0
                        db_sessioni_daily_op.at[j, 'PayableTotal'] = db_sessioni_daily_op.at[j - 1, 'PayableTotal'] + 900
                        db_sessioni_daily_op.at[j, 'Payable'] = 900
                    elif payable_tamporary < 7200:
                        payable_tamporary = 0
                        db_sessioni_daily_op.at[j, 'PayableTotal'] = db_sessioni_daily_op.at[j - 1, 'PayableTotal']
                        db_sessioni_daily_op.at[j, 'Payable'] = 0

        db_bonificato = pd.concat([db_bonificato, db_sessioni_daily_op]).reset_index(drop=True)

    db_bonificato['SessionLengthHours'] = db_bonificato['SessionLength'] / 3600
    db_bonificato['PayableTotalHours'] = db_bonificato['PayableTotal'] / 3600
    db_bonificato['PayableHours'] = db_bonificato['Payable'] / 3600

    return db_bonificato.drop('WorkDay', axis=1)


def ore_lista(campagna, lista, giorno):
    giorno_fix = pd.to_datetime(giorno, format="%d/%m/%Y").strftime("%Y-%m-%d")
    calls_del_giorno = pd.read_sql(f'SELECT ContactId, CallID, NameOverride, CallStarDate '
                                   f'FROM calls '
                                   f'INNER JOIN contacts ON calls.ContactId=contacts.ContactId '
                                   f'WHERE CallStarDate = \'{giorno_fix}\'')

    filt_campagna = (calls_del_giorno['NameOverride'] == campagna)
    filt_lista = (calls_del_giorno['NameOverride'] == campagna) & (calls_del_giorno['ContactListName'] == lista)
    calls_totali = len(calls_del_giorno[filt_campagna])
    calls_lista = len(calls_del_giorno[filt_lista])

    lista_operatori = calls_del_giorno[filt_lista]['Username'].drop_duplicates()
    lista_operatori.loc[lista_operatori['Username'].isnull(), 'Username'] = "MACCHINA"
    for i in range(0, len(lista_operatori)):
        if str(lista_operatori[i]) in ["MACCHINA", "Test Across", "nan"]:
            lista_operatori = lista_operatori.drop([i])
    lista_operatori = lista_operatori.drop_duplicates().reset_index(drop=True)

    if calls_totali == 0:
        perc_calls = 0
    else:
        perc_calls = calls_lista / calls_totali

    sessioni_del_giorno = pd.read_sql(f'SELECT *'
                                      f' FROM sessions'
                                      f' WHERE StartDate=\'{giorno_fix}\'')

    filt_operatori = (sessioni_del_giorno['username'].isin(lista_operatori))
    sessioni_utili = sessioni_del_giorno[filt_operatori]

    filt_retribuito = (sessioni_utili['Name'] != 'Pausa Non Retribuita')
    ore_totali = sessioni_utili[filt_retribuito]['SessionLengthHours'].sum()

    return ore_totali * perc_calls

