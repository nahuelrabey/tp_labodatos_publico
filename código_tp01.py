# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 14:21:17 2024

@author: Sammy
"""
#%%===========================================================================
# Importamos bibliotecas
#=============================================================================

import pandas as pd
import duckdb
import numpy as np 

#%%===========================================================================
# Importamos los datasets que vamos a utilizar en este programa, los limpiamos para que no tengan NaN
#=============================================================================

carpeta = './enunciado_tablas/'
carpeta_finales = './tablas_finales/'

equipo_e = pd.read_csv(carpeta+'enunciado_equipos.csv').dropna()

liga_e = pd.read_csv(carpeta+'enunciado_liga.csv').dropna()

#Este es el dataset más importante, permite relacionar casi todas las entidades
partido_e = pd.read_csv(carpeta+'enunciado_partidos.csv').dropna() 

jugador_e = pd.read_csv(carpeta+'enunciado_jugadores.csv').dropna()

atributos_e = pd.read_csv(carpeta+'enunciado_jugadores_atributos.csv').dropna()

#%%===========================================================================
# Creeamos las tablas 
#=============================================================================

#Eliminamos las columnas que no queremos 
equipo_ok = equipo_e.drop(['team_fifa_api_id','team_short_name'], axis=1)

#Usando el dataset partido, creamos un DF donde en una columna estén los equipos locales y visitantes y en otra su liga 
equipo_liga = pd.melt(partido_e, 
                        id_vars=['league_id', 'season'], 
                        value_vars=['home_team_api_id', 'away_team_api_id'],
                        value_name='team_id').drop(columns='variable')

# Al DF creado, le eliminamos los duplicados 
equipo_liga = equipo_liga[['team_id', 'league_id']].drop_duplicates()

#Usamos SQL unir el DF creado con el dataset de partidos

consulta_equipo= '''SELECT *
                    FROM equipo_liga
                    INNER JOIN equipo_ok
                    ON team_id = team_api_id'''
equipo_0 = duckdb.sql(consulta_equipo)

#Creamos la tabla final 

consulta = '''SELECT team_id AS Id_Equipo, league_id AS Id_Liga, team_long_name AS Nombre FROM equipo_0'''
equipo = duckdb.sql(consulta).df()

#Guardamos la tabla
equipo.to_csv(carpeta_finales + 'equipo.csv', index=False) 

#%% LIGA 

consulta_liga = '''SELECT country_id as Id_Liga, name FROM liga_e '''

liga = duckdb.sql(consulta_liga).df()
liga.to_csv(carpeta_finales + "liga.csv")

#%% Partido

#Creamos una tabla solo con los atributos necesarios
consulta_partido = '''SELECT match_api_id AS Id_Partido, date AS Fecha, season AS Temporada, home_team_api_id AS Id_Local,
                       away_team_api_id AS Id_Visitante, home_team_goal, away_team_goal 
                       FROM partido_e'''

partido_ok = duckdb.sql(consulta_partido).df()

#Necesitamos crear una columna con el resultado del partido, así que restamos las columnas del gol local y visitante
partido_ok['Valor'] = partido_ok['home_team_goal']-partido_ok['away_team_goal']

#Definimos un clasificador para los valores de la resta anterior. El resultado será el del equipo local
def clasificador(x):
    if x > 0:
        return 'Ganado'
    elif x < 0:
        return 'Perdido'
    else:
        return 'Empate'

partido_ok['Resultado'] = partido_ok['Valor'].apply(clasificador)

partido = duckdb.sql('''SELECT Id_Partido, Fecha, Temporada,  Id_Local,
                        Id_Visitante, Resultado FROM partido_ok''').df()

partido.to_csv(carpeta_finales + "./partido.csv")
                      
#%% PLANTEL POR AÑO 

# Seleccionamos los partidos de la temporada 2015
partidos_2015 = partido_e[partido_e["season"] == "2015/2016"]

# Creamos un DF con los jugadores locales, su id y equipo

local_0 = pd.melt(partidos_2015, 
                id_vars=['match_api_id', 'home_team_api_id'],
                value_vars=[f'home_player_{i}' for i in range(1, 12)],
                value_name='Id_jugador').drop(columns='variable') 

local = duckdb.sql('''SELECT home_team_api_id AS Id_Equipo, Id_jugador, match_api_id AS Id_partido FROM local_0 ''').df()

# Idem jugadores visitantes

visitante_0 = pd.melt(partidos_2015, 
                id_vars=['match_api_id', 'away_team_api_id'],
                value_vars=[f'away_player_{i}' for i in range(1, 12)],
                value_name='Id_jugador').drop(columns='variable') 

visitante = duckdb.sql('''SELECT away_team_api_id AS Id_Equipo, Id_jugador, match_api_id AS Id_partido FROM visitante_0 ''').df()

#Unimos los DF y eliminamos duplicados
jugadores = pd.concat([local, visitante], ignore_index=True) 

plantel_0 = jugadores[['Id_Equipo', 'Id_jugador']].drop_duplicates()

plantel = plantel_0.sort_values(by='Id_Equipo')

#%% PLANTEL A LO LARGO DE LAS TEMPORADAS

# Creamos un DF con los jugadores locales, su id y equipo

local_0 = pd.melt(partido_e, 
                id_vars=['match_api_id', 'home_team_api_id','season'],
                value_vars=[f'home_player_{i}' for i in range(1, 12)],
                value_name='Id_Jugador').drop(columns='variable') 

local = duckdb.sql('''SELECT home_team_api_id AS Id_Equipo, Id_Jugador, match_api_id AS Id_partido, season AS Temporada FROM local_0 ''').df()

# Idem jugadores visitantes

visitante_0 = pd.melt(partido_e, 
                id_vars=['match_api_id', 'away_team_api_id','season'],
                value_vars=[f'away_player_{i}' for i in range(1, 12)],
                value_name='Id_Jugador').drop(columns='variable') 

visitante = duckdb.sql('''SELECT away_team_api_id AS Id_Equipo, Id_Jugador, match_api_id AS Id_partido,season AS Temporada FROM visitante_0 ''').df()

#Uno los DF y elimino duplicados
jugadores = pd.concat([local, visitante], ignore_index=True) 

plantel_0 = jugadores[['Id_Jugador', 'Id_Equipo','Temporada']].drop_duplicates()

plantel = plantel_0.sort_values(by=['Id_Equipo','Temporada'])

#%%

local_0 = pd.melt(partido_e, 
                id_vars=['match_api_id', 'home_team_api_id','season'],
                value_vars=[f'home_player_{i}' for i in range(1, 12)],
                value_name='Id_jugador').drop(columns='variable') 

local = duckdb.sql('''SELECT home_team_api_id AS Id_Equipo, Id_jugador, match_api_id AS Id_partido, season AS Temporada FROM local_0 ''').df()

# Idem jugadores visitantes

visitante_0 = pd.melt(partido_e, 
                id_vars=['match_api_id', 'away_team_api_id', 'season'],
                value_vars=[f'away_player_{i}' for i in range(1, 12)],
                value_name='Id_jugador').drop(columns='variable') 

visitante = duckdb.sql('''SELECT away_team_api_id AS Id_Equipo, Id_jugador, match_api_id AS Id_partido, season AS Temporada FROM visitante_0 ''').df()

#Uno los DF y elimino duplicados
jugadores = pd.concat([local, visitante], ignore_index=True) 

plantel_0 = jugadores[['Id_jugador','Id_Equipo', 'Temporada']].drop_duplicates()

plantel = plantel_0.sort_values(by=['Id_Equipo','Temporada'])
plantel.to_csv(carpeta_finales + "plantel.csv")








