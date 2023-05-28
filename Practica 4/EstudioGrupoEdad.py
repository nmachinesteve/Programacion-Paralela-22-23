
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit 
from pyspark.sql.types import BooleanType
from pyspark.sql.types import StructType, IntegerType,StructField, FloatType
import pyspark.sql.functions as fifa

import matplotlib.pyplot as plt
from matplotlib import colors 
from pylab import *

import time
import curses

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

FILES = ['201901_Usage_Bicimad.json',
    '201902_Usage_Bicimad.json',
    '201903_Usage_Bicimad.json',
    '201904_Usage_Bicimad.json',
    '201905_Usage_Bicimad.json',
    '201906_Usage_Bicimad.json',
    '201907_movements.json',
    '201908_movements.json',
    '201909_movements.json',
    '201910_movements.json',
    '201911_movements.json',
    '201912_movements.json']

def columnas(dfs):
    """
    Seleccionamos el data  frame con las variables que más nos interesan.
    Devuelbe el dataframe con las columnas seleccionadas.

    """
    for i in range(len(dfs)):
        
        dfs[i] = dfs[i].select('ageRange', 'idplug_station', 
                               'idunplug_station', 'travel_time')
    return dfs

# union de una lista de DataFrames 
def union(dfs):
    df_union = dfs[0]
    for i in range(11):
        df_union = df_union.union(dfs[i+1])
    return df_union

def count_age(df1):
    """
    Devuelve un data frame con la cantidad de veces que cada grupp de edad
    usa biciMad, además devuelve el del mayor y menor valor junto con los 
    valores obtenidos
    """
    df = df1.groupBy('ageRange').count()
    df = df.orderBy('ageRange')
    x = df.rdd.map(lambda x: x['ageRange']).collect()
    y = df.rdd.map(lambda x: x['count']).collect()
    
    mx = df.rdd.map(lambda x: x['count']).max()
    dfMax = df.filter(df['count'] == mx)
    a = dfMax.select('ageRange').first()[0]
    
    mn = df.rdd.map(lambda x: x['count']).min()
    dfMin = df.filter(df['count'] == mn)
    b = dfMin.select('ageRange').first()[0]
    
    
    return df,x,y, a, b, mx, mn


def resultados_descriptivo_age(dac,count_use_max, count_use_min, mx, mn,name,measure):
    
    print('Tabla de frecuencias de usuarios dividido en Grupo de Edad del año 2019: ')
    dac.show()
        
    print(f'Grupo de Edad con mayor {name} en 2019  ha sido', count_use_max, 
          'con un total de', mx, measure)
    if mn != -1:
        print(f'Grupo de Edad con menor {name} en 2019  ha sido', count_use_min, 
              'con un total de', mn, f'{measure}\n')
  
def grafico_barras(title,x,y,et1,et2):
    curses.wrapper(mostrar_grafica,title,'b', x, y, et1, et2)
    
def grafico_sectores(title,x,y,et1,et2):
    curses.wrapper(mostrar_grafica,title,'s', x, y, et1, et2)
  
def mostrar_grafica(stdscr, title,cual, x, y, et1, et2):
    stdscr.clear()
    curses.curs_set(0)  
    stdscr.nodelay(1)  
    stdscr.timeout(0)
    
    if cual == 'b':
        plt.bar(x, y, color='#5d9b9b', ls='-')
        plt.title(title)
        plt.xlabel(et1)
        plt.ylabel(et2)
        plt.draw()
    else:
        plt.pie(y)
        plt.title(title)
        plt.legend(x)
        plt.draw()
    
    start_time = time.time()
    while True:
        elapsed_time = time.time() - start_time
        if elapsed_time >= 5:
            break
        
        if stdscr.getch() != -1:
            break
        
        
        plt.pause(0.1)
    
    plt.close()
    stdscr.clear()
    curses.curs_set(1)
    stdscr.nodelay(0)
    stdscr.timeout(-1)
    
def porcent_count_age(df):
    """
    Devuelve una lista con el porcentaje de veces que 
    cada age range usa bicimad en una lista ordenada
    """
    porcentaje = [0]*7
    for i in [0,1,2,3,4,5,6]:
        n = df.filter(df['ageRange']== i).count()
        l = df.count() 
        porcentaje[i] = round(n/l *100, 2)
    return porcentaje
 
def estudio_porcent(df,f):
    porcentaje = f(df)
    age = [0,1,2,3,4,5,6]
    schema = StructType([
            StructField('ageRange', IntegerType(), nullable=False),
            StructField('count percentage', FloatType(), nullable=False)
            ])
    tuplas = [(valor1, valor2) for valor1, valor2 in zip(age, porcentaje)]
    df_por = spark.createDataFrame(tuplas, schema) 
    df_por.show()
    
    grafico_sectores('Porcentaje por cada Grupo de edad',age,porcentaje,0,0)
    
def buscar_max_age(df,var):
    """
    Dado el date frame devuelve el data frame con los tiempo maximos de cada grupo de edad
    y el grupo de edad que haya hecho el viaje mas largo 
    """
    df = df.select('ageRange',var)
    dfmax = df.groupBy('ageRange').max() # Df con los tiempos max en funcion de la edad
    dfmax = dfmax.orderBy('ageRange')
    dfmax = dfmax.na.drop(subset=['ageRange'])
    
    mx = dfmax.rdd.map(lambda x: x[f'max({var})']).max() #El tiempo max de dfmax
    dfMax = dfmax.filter(dfmax[f'max({var})'] == mx)
    a = dfMax.select('ageRange').first()[0]
    
    return dfmax,a, mx
    
def buscar_media_total(df,var):
    total = df.rdd.map(lambda x: x[f'{var}']).sum()
    long = df.count()
    media = total/long
    return media

def buscar_mediaVar_age(df,var):
    """
    Obtenemos un dataframe con las medias de la variable 'var'
    para cada grupo de edad, y devolvemos el maximo y el minimo
    """
    df = df.select('ageRange',var)
    df = df.groupBy('ageRange').mean(var)
    df = df.orderBy('ageRange')
    x = df.rdd.map(lambda x: x['ageRange']).collect()
    y = df.rdd.map(lambda x: x[f'avg({var})']).collect()
    
    mx = df.rdd.map(lambda x: x[f'avg({var})']).max()
    dfMax = df.filter(df[f'avg({var})'] == mx)
    a = dfMax.select('ageRange').first()[0]
    
    mn = df.rdd.map(lambda x: x[f'avg({var})']).min()
    dfMin = df.filter(df[f'avg({var})'] == mn)
    b = dfMin.select('ageRange').first()[0]
    
    return df,x,y,a,b,mx,mn

def porcent_age_time(df):
    """
    Devuelve una lista con el porcentaje de tiempo que 
    cada age range usa bicimad en una lista ordenada

    """
    n = 7
    df = df.groupBy('ageRange').sum() # data frame que a cada age range le da el numero total de segundos que han usado bicimad
    df = df.select('ageRange', 'sum(travel_time)') 
    df = df.orderBy('ageRange') #Ordeno el df en funcion de el age range
    
    l0 = df.select('sum(travel_time)').take(n)
    l1 = []
    total = 0 
    for i in range(n):
        l1.append(l0[i][0])
        total += l0[i][0]
    
    porcentaje = []
    for i in range(n):
        porcentaje.append((l1[i]/total)*100)
        
    return porcentaje 

def station_min_max(df,n):
    if n == 0:
        return station_min_max1(df, 'idplug_station')
    
    else:  
        return station_min_max1(df, 'idunplug_station')

def station_min_max1(df, var):
    df = df.orderBy('count')
    mn = df.select(f'{var}').take(1)[0][0]     
    l = df.select(fifa.collect_list(f'{var}')).first()[0]
    n = len(l)
    mx = l[n-1]
    
    if var == 'idunplug_station':
        print('La estación de salida menos usada es la numero ', mn, 
              ' y la mas concurrida es ', mx)
    if var == 'idplug_station':
        print('La estación de llegada menos usada es la numero ', mn, 
              ' y la mas concurrida es ', mx)
    return mn,mx

def main(files):
    dfs = []
    for f in files:
        dfs.append(spark.read.json(f))
     
    dfs1 = columnas(dfs)
    df_unionPre = union(dfs1)
    
    # Limpieza de datos
    df_union = df_unionPre.na.drop(subset=['ageRange']) # quita valores null en age
    df_union = df_union.filter(df_union['travel_time']>=0) # filtro por tiempo positivo en el DataFrame
    df_union = df_union.filter(df_union['travel_time']<86400) #filtro por tiempo menor a un día
    
    # Descriptivo VECES
    dac,age,age_count_use, age_count_use_max, age_count_use_min, mxa, mna = count_age(df_union)
    resultados_descriptivo_age(dac,age_count_use_max, age_count_use_min, mxa, mna,"uso","veces")
    grafico_barras("Veces por Grupo de Edad",age,age_count_use, "Grupo de Edad", "veces")  
    
    estudio_porcent(df_union,porcent_count_age)
    
    # Estudio sobre TIEMPO recorrido por Grupo de Edad
    max_age, max_age_group, max_age_time = buscar_max_age(df_union, 'travel_time')
    resultados_descriptivo_age(max_age, max_age_group, "", max_age_time, -1,"tiempo","segundos")
    
    # Estudio sobre tiempo recorrido medio por usuario y por Grupo de Edad
    mean_timetravel = buscar_media_total(df_union, 'travel_time')
    print("\n La media de tiempo de uso respecto al total de usuarios es: ",  mean_timetravel, "segundos \n")
    
    dfmean,age,mtimetravel ,mean_timetravel_max, mean_timetravel_min, mxt, mnt  = buscar_mediaVar_age(df_union,'travel_time')
    resultados_descriptivo_age(dfmean,mean_timetravel_max, mean_timetravel_min, mxt, mnt,"tiempo medio","segundos")
    grafico_barras("Tiempo medio recorrido por Grupo de Edad",age,mtimetravel, "Grupo de Edad", "segundos") 
    
    estudio_porcent(df_union,porcent_age_time)
    
    # Estudio sobre estaciones ESTACIONES más concurridas por los usuarios
    df_plug = df_union.groupBy('idplug_station').count()
    df_unplug = df_union.groupBy('idunplug_station').count()
    
    station_min_max(df_plug,0)
    station_min_max(df_unplug,1)
    
    

    


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    main(FILES)
    spark.stop()



