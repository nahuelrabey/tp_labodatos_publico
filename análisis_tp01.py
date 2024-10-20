# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 17:32:46 2024

@author: Sammy
"""

#%% Importamos bibliotecas
import pandas as pd
import duckdb
import numpy as np 
import seaborn as sns
import matplotlib.pyplot as plt

#%%===========================================================================
# Importamos los datasets que vamos a utilizar en este programa, los limpiamos para que no tengan NaN
#=============================================================================

carpeta = r'./tablas_finales/'

equipo = pd.read_csv(carpeta+'equipo_españa.csv')
plantel = pd.read_csv(carpeta+'plantel.csv')
partido = pd.read_csv(carpeta+'partido_españa.csv')
jugador = pd.read_csv(carpeta+'jugador.csv')

#%% Determinamos un criterio de selección de temporadas NO SÉ QUE QUERÍA HACER

#Vemos si cada temporada tiene igual número de equipos 

equipos_x_temporada = partido.groupby('Temporada')['Id_Local'].nunique()

# Check if each season has exactly 20 teams
igual_num_equipos = equipos_x_temporada.nunique() == 1

# Print the result
if igual_num_equipos:
    print("Cada temporada tiene igual número de equipos.")
else:
    print(equipos_x_temporada) 
#%%
#Vemos cuántos partidos se jugaron durante la temporadas
partidos_temp1 = partido[partido['Temporada']=='2008/2009']
partidos_temp2 = partido[partido['Temporada']=='2012/2013']
num_partidos_temp = partido['Temporada'].value_counts().sort_index()

#Definimos 4 temporadas consecutivas 

temporadas_0 = num_partidos_temp.loc['2008/2009':'2011/2012']
temporadas_1 = num_partidos_temp.loc['2009/2010':'2012/2013']
temporadas_2 = num_partidos_temp.loc['2010/2011':'2013/2014']
temporadas_3 = num_partidos_temp.loc['2011/2012':'2014/2015']
temporadas_4 = num_partidos_temp.loc['2012/2013':'2015/2016']

#Vemos la diferencia entre el número de partidos jugamos entre temporada
diferencia_0 = temporadas_0.diff()
diferencia_1 = temporadas_1.diff()
diferencia_2 = temporadas_2.diff()
diferencia_3 = temporadas_3.diff()
diferencia_4 = temporadas_4.diff()

diferencias = (diferencia_0,diferencia_1,diferencia_2,diferencia_3,diferencia_4)

#Decidimos que un criterio puede ser las temporadas con una diferencia de partidos entre ellas menor a 20
for i in diferencias:
    menos_20 = abs(i) < 20
    print(menos_20) 

#%%
#Seleccionamos las temporadas que van desde 2011/2012 hasta el 2014/2015

equipos_jugaron = np.unique(partidos_temp1["Id_Visitante"].to_numpy())
equipos_jugaron2 = np.unique(partidos_temp2["Id_Local"].to_numpy())


#%%Creamos las tablas con las temporadas de interés

partido_ok =  partido[(partido['Temporada'] >= '2011/2012') & (partido['Temporada'] <= '2014/2015')]
plantel_ok =  plantel[(plantel['Temporada'] >= '2011/2012') & (plantel['Temporada'] <= '2014/2015')]

equipo_0 = duckdb.sql('''SELECT * FROM equipo INNER JOIN partido_ok ON Id_Equipo = Id_Local OR Id_Equipo = Id_Visitante''').df()
equipo_ok = equipo_0[['Id_Equipo', 'Nombre', 'Temporada']].drop_duplicates()

#%%===========================================================================
# CONSULTASS SQL
#=============================================================================
#%%Equipo más ganador en general 

#Creamos una tabla con el Id del equipo que ganó cada partido de cada temporada
consulta_victorias = '''SELECT Id_Local AS Id_Equipo
                        FROM partido_ok
                        WHERE Gol_local > Gol_visitante
                        UNION ALL
                        SELECT Id_Visitante AS Id_Equipo
                        FROM partido_ok
                        WHERE Gol_visitante > Gol_local'''
equipo_ganador_0 = duckdb.sql(consulta_victorias).df()                    

#Contamos el número de veces que se repite cada Id y pedimos el máximo
consulta_ganador = '''SELECT Id_Equipo, COUNT(*) AS Victorias
                      FROM equipo_ganador_0
                      GROUP BY Id_Equipo
                      ORDER BY Victorias DESC
                      LIMIT 1
                       '''  
equipo_ganador_1 = duckdb.sql(consulta_ganador).df()

consulta_nombre = '''SELECT DISTINCT Nombre 
                       FROM equipo_ok AS e
                       INNER JOIN equipo_ganador_1 AS p
                       ON e.Id_Equipo = p.Id_equipo
                       '''
equipo_ganador = duckdb.sql(consulta_nombre).df()

#%%Equipo más perdedor por AÑO

#Cada temporada no es un año calendario, por lo que debemos modificar nuestra tabla de partido
#para que incluya a las temporadas 2010/2011 y 2015/2016 puesto que comparten transcurren también en los años que nos interesan 

partido_ok_por_año =  partido[(partido['Temporada'] >= '2010/2011') & (partido['Temporada'] <= '2015/2016')]

equipo_0_por_año = duckdb.sql('''SELECT * FROM equipo INNER JOIN partido_ok_por_año ON Id_Equipo = Id_Local OR Id_Equipo = Id_Visitante''').df()
equipo_ok_por_año = equipo_0_por_año[['Id_Equipo', 'Nombre', 'Temporada']].drop_duplicates()

#Creamos una tabla con el Id del equipo que perdió cada partido de cada temporada
consulta_derrotas = '''SELECT Id_Local AS Id_Equipo, Fecha
                        FROM partido_ok_por_año
                        WHERE Gol_local < Gol_visitante
                        UNION ALL
                        SELECT Id_Visitante AS Id_Equipo, Fecha
                        FROM partido_ok_por_año
                        WHERE Gol_visitante < Gol_local'''
                        
equipo_perdedor_0 = duckdb.sql(consulta_derrotas).df() 

#Nos interesa saber el año, así que lo extraemos de la fecha
consulta_año = '''SELECT *, EXTRACT(YEAR FROM CAST(Fecha AS DATE)) AS Año
FROM equipo_perdedor_0'''

equipo_perdedor_año = duckdb.sql(consulta_año).df()

#Contamos el número de veces que se repite cada Id para cada AÑO
consulta_perdedor = '''SELECT Id_Equipo, Año, COUNT(*) AS Derrotas
                      FROM equipo_perdedor_año
                      GROUP BY Id_Equipo,Año
                      ORDER BY Año, Derrotas DESC
                
                       '''  
equipo_perdedor_1 = duckdb.sql(consulta_perdedor).df()

#Buscamos el equipo con más derrotas en cada AÑO            
consulta_max_perdedor = ''' SELECT Año, Id_Equipo, Derrotas
                            FROM equipo_perdedor_1 AS ep
                            WHERE Derrotas = (SELECT MAX(Derrotas)
                            FROM equipo_perdedor_1
                            WHERE Año = ep.Año)
                            ORDER BY Año'''
                            
equipo_max_perdedor = duckdb.sql(consulta_max_perdedor).df()

consulta_nombre_perdedor = '''SELECT DISTINCT Nombre, p.Año 
                       FROM equipo_ok_por_año AS e
                       INNER JOIN equipo_max_perdedor AS p
                       ON e.Id_Equipo = p.Id_equipo
                       ORDER BY Año ASC
                       '''
                       
equipo_perdedor = duckdb.sql(consulta_nombre_perdedor).df()


#%% Equipo con más empates del último año 
#De nuevo, agregamos las temporadas 2010/2011 y 2015/2016 puesto que comparten transcurren también en los años que nos interesan 

partido_por_año =  partido[(partido['Temporada'] >= '2010/2011') & (partido['Temporada'] <= '2015/2016')]

#Extraemos la información del año en que se juegan los partidos

consulta_por_año = '''SELECT *, EXTRACT(YEAR FROM CAST(Fecha AS DATE)) AS Año
FROM partido_por_año'''

partido_ok_por_año = duckdb.sql(consulta_por_año).df()

#Estudiamos la columna resultado y nos quedamos solo con el último año
consulta_empate = '''SELECT Id_Local AS Id_Equipo, Año
                     FROM partido_ok_por_año 
                     WHERE Resultado = 'Empate'
                     UNION ALL 
                     SELECT Id_Visitante AS Id_Equipo, Año
                     FROM partido_ok_por_año 
                     WHERE Resultado = 'Empate' AND Año = 2015
                    '''
equipos_empate = duckdb.sql(consulta_empate).df()

#Contamos el número de veces que se repite cada Id durante el año
consulta_empate_ok = '''SELECT Id_Equipo, COUNT(*) AS Empates
                      FROM equipos_empate
                      GROUP BY Id_Equipo
                      ORDER BY Empates DESC
                      LIMIT 1
                       '''  
equipo_empate = duckdb.sql(consulta_empate_ok).df()



consulta_nombre = '''SELECT DISTINCT Nombre 
                       FROM equipo_ok_por_año AS e
                       INNER JOIN equipo_empate AS p
                       ON e.Id_Equipo = p.Id_equipo
                       '''
equipo_empate_2015 = duckdb.sql(consulta_nombre).df()

#%%Equipo con más goles a favor 

consulta_gol_favor = '''SELECT Id_Local AS Id_Equipo, Gol_local AS Goles_a_favor
                        FROM partido_ok
                        UNION ALL
                        SELECT Id_Visitante AS Id_Equipo, Gol_visitante AS Goles_a_favor
                        FROM partido_ok'''

equipo_goles_por_fecha = duckdb.sql(consulta_gol_favor).df()

# Sumamos los goles que medió cada equipo
consulta_goles = '''SELECT Id_Equipo, SUM(Goles_a_favor) AS Goles_a_favor
                    FROM equipo_goles_por_fecha
                    GROUP BY Id_Equipo
                    ORDER BY Goles_a_favor DESC
                    LIMIT 1
                    '''
                    
equipo_max_goles_favor = duckdb.sql(consulta_goles).df()

consulta_nombre = '''SELECT DISTINCT Nombre 
                     FROM equipo_ok AS e
                     INNER JOIN equipo_max_goles_favor AS p
                     ON e.Id_Equipo = p.Id_Equipo'''

equipo_goles_a_favor = duckdb.sql(consulta_nombre).df()


#%% Equipo con mayor diferencia de goles 
consulta_gol_diff = '''SELECT Id_Local AS Id_Equipo, (Gol_local-Gol_visitante) AS Goles_a_favor
                        FROM partido_ok
                        UNION ALL
                        SELECT Id_Visitante AS Id_Equipo, (Gol_visitante-Gol_local) AS Goles_a_favor
                        FROM partido_ok
                        '''
                        
equipo_gol_diff_por_fecha = duckdb.sql(consulta_gol_diff).df()

#Contamos el número de veces que se repite cada Id y pedimos el máximo
consulta_gol_diff_ok = '''SELECT Id_Equipo, SUM(Goles_a_favor) AS Gol_diferencia
                      FROM equipo_gol_diff_por_fecha
                      GROUP BY Id_Equipo
                      ORDER BY Gol_diferencia DESC
                      LIMIT 1
                     
                       '''  
equipo_max_gol_diff = duckdb.sql(consulta_gol_diff_ok).df()

consulta_nombre = '''SELECT DISTINCT Nombre 
                       FROM equipo_ok AS e
                       INNER JOIN equipo_max_gol_diff AS p
                       ON e.Id_Equipo = p.Id_equipo
                       '''
equipo_gol_diff = duckdb.sql(consulta_nombre).df()

#%% Número de jugadores de cada equipo 
consulta_num_jugadores = '''SELECT Id_Equipo, COUNT(Id_Jugador) AS Número_jugadores
                            FROM plantel_ok
                            GROUP BY Id_Equipo'''
num_jugadores = duckdb.sql(consulta_num_jugadores).df()

consulta_nombre = '''SELECT DISTINCT Nombre, Número_jugadores 
                       FROM equipo_ok AS e
                       INNER JOIN num_jugadores AS p
                       ON e.Id_Equipo = p.Id_equipo
                       ORDER BY Número_jugadores DESC
                       '''
num_jugadores_por_equipo = duckdb.sql(consulta_nombre).df()

#%% Jugadores que más partidos ganó su equipo

#Ya tenemos una tabla con el equipo que más partidos ganó en general, nos fijamos entonces en los jugadores
#que pertenecieron a ese equipo

consulta_jugadores_mas_ganadores = '''SELECT DISTINCT Id_Jugador 
                                      FROM plantel_ok 
                                      WHERE Id_Equipo = 8634'''
                                      
jugadores_mas_ganadores_0 = duckdb.sql(consulta_jugadores_mas_ganadores).df()

consulta_nombre = '''SELECT DISTINCT Nombre
                       FROM jugador AS e
                       INNER JOIN jugadores_mas_ganadores_0 AS p
                       ON e.Id_Jugador = p.Id_Jugador
                       '''
                       
jugadores_mas_ganadores = duckdb.sql(consulta_nombre).df()
lista_jugadores_mas_ganadores = np.array(jugadores_mas_ganadores['Nombre']).tolist()

#%%Jugador que estuvo en más equipos

#Contamos a cuantos equipos a perten
consulta_mas_equipos = '''SELECT Id_Jugador,COUNT(DISTINCT Id_Equipo) AS num_equipos
                          FROM plantel_ok
                          GROUP BY Id_Jugador
                          ORDER BY num_equipos DESC
                          LIMIT 1'''
                          
jugador_mas_equipos_0 = duckdb.sql(consulta_mas_equipos).df()

consulta_nombre = '''SELECT DISTINCT Nombre
                       FROM jugador AS e
                       INNER JOIN jugador_mas_equipos_0 AS p
                       ON e.Id_Jugador = p.Id_Jugador
                       '''
                       
jugador_mas_equipos = duckdb.sql(consulta_nombre).df()

#%%===========================================================================
# Visualizaciones
plt.rcParams["figure.figsize"] = (8,6)
plt.rcParams['font.size'] = 12            
plt.rcParams['font.family'] = 'Verdana' 
plt.rcParams['axes.labelsize'] = 14       
plt.rcParams['axes.titlesize'] = 16      
plt.rcParams['legend.fontsize'] = 14    
plt.rcParams['xtick.labelsize'] = 10.5      
plt.rcParams['ytick.labelsize'] = 10.5 
#=============================================================================
#%%Goles de local y visitante

consulta_gol = '''SELECT Id_Equipo, SUM(Gol_local) AS Gol_local, SUM(Gol_visitante) AS Gol_visitante
                  FROM (SELECT Id_Local AS Id_Equipo, Gol_local, 0 AS Gol_visitante
                        FROM partido_ok
                        UNION ALL 
                        SELECT Id_Visitante AS Id_Equipo, 0 AS Gol_local, Gol_visitante
                        FROM partido_ok) AS goles
                   GROUP BY Id_Equipo
                   ORDER BY Gol_local DESC'''
                  
gol_local_visitante = duckdb.sql(consulta_gol).df()

consulta_nombre = '''SELECT DISTINCT Nombre, p.Gol_local, p.Gol_visitante
                       FROM equipo_ok AS e
                       INNER JOIN gol_local_visitante AS p
                       ON e.Id_Equipo = p.Id_equipo
                       ORDER BY Gol_local ASC
                       '''
                       
equipo_gol_local_visitante = duckdb.sql(consulta_nombre).df()

# Gráfico
fig, ax = plt.subplots()
       
equipo_gol_local_visitante.plot(x='Nombre', 
                    y=['Gol_local', 'Gol_visitante'], 
                    kind='barh',
                    label=['Gol Visitante', 'Gol Local'],   # Agrega etiquetas a la serie
                    ax = ax,stacked=True)
ax.set_title('Goles convertidos por equipo según si jugó como local o visitante')
ax.set_xlabel('Goles')
ax.set_ylabel('')

fig.savefig(carpeta+'goles_convertidos.pdf', bbox_inches='tight', pad_inches=0.1)