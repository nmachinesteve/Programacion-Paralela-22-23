
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit 
from pyspark.sql.types import BooleanType, FloatType
from pyspark.sql.types import StructType, IntegerType,StructField
import pyspark.sql.functions as fifa


import matplotlib.pyplot as plt
from matplotlib import colors 
from pylab import *


import time
import curses

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

months = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 
            'septiembre', 'octubre', 'noviembre', 'diciembre']

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
    Añade las columnas de mes y estación del año que nos interesa para el estudio,
    además seleccionamos el data  frame con las variables que más nos interesan.
    Devuelbe el dataframe con las columnas seleccionadas.

    """
    m = 1
    for i in range(len(dfs)):
        if m < 3:
            s = 'winter'
        elif m < 6:
            s = 'spring'
        elif m < 9:
            s = 'summer'
        else:
            s = 'autumn'
        dfs[i] = dfs[i].withColumns({'month':lit(m), 'season':lit(s)})
        dfs[i] = dfs[i].select('month', 'season', 'ageRange', 'idplug_station', 
                               'idunplug_station', 'travel_time')
        m += 1
    return dfs


def union(dfs):
    """
    Unión de todos los data frames de los distintos archivos en uno solo.

    """
    df_union = dfs[0]
    for i in range(11):
        df_union = df_union.union(dfs[i+1])
    return df_union

def count_var(df1,var):
    """
    Devuelve un data frame con la cantidad de veces que cada grupp de edad
    usa biciMad, además devuelve el del mayor y menor valor junto con los 
    valores obtenidos
    """
    df = df1.groupBy(var).count()
    df = df.orderBy(f'{var}')
    x = df.rdd.map(lambda x: x[f'{var}']).collect()
    y = df.rdd.map(lambda x: x['count']).collect()
    
    mx = df.rdd.map(lambda x: x['count']).max()
    dfMax = df.filter(df['count'] == mx)
    a = dfMax.select(f'{var}').first()[0]
    
    mn = df.rdd.map(lambda x: x['count']).min()
    dfMin = df.filter(df['count'] == mn)
    b = dfMin.select(f'{var}').first()[0]
    
    
    return df,x,y, a, b, mx, mn

def porcent_count_age(df1):
    seasons = ['winter','spring','summer','autumn']
    lista = []
    for i in range(4) :
        df= df1.filter(df1['season']==seasons[i])
        lista.append([seasons[i],porcent_count_age1(df)])
    return lista

def porcent_count_age1(df):
    porcentaje = [0]*7
    age = [0,1,2,3,4,5,6]
    for i in age:
        n = df.filter(df['ageRange']== i).count()
        l = df.count() # Devuelve el numero de datos que tenemos 
        porcentaje[i] = round(n/l *100, 2)
    return porcentaje

def resultados_descriptivo(name, dac,x,y,count_use_max, count_use_min, mx, mn):
    
    print(f'Tabla de frecuencias de usuarios dividido en {name} del año 2019: ')
    dac.show()
        
    print(f'{name} con mayor uso en 2019  ha sido', count_use_max, 
          'con un total de', mx, 'veces.')
    print(f'{name} con menor uso en 2019  ha sido', count_use_min, 
          'con un total de', mn, 'veces \n')

def df_show(list_porc):
    age = [0,1,2,3,4,5,6]
    for i in list_porc:
        schema = StructType([
            StructField('ageRange', IntegerType(), nullable=False),
            StructField('count percentage', FloatType(), nullable=False)
            ])
        
        tuplas = [(valor1, valor2) for valor1, valor2 in zip(age, i[1])]
        print('Los porcentajes en funcion del count y del ageRange según la estacion', i[0], 'es:')
        df_por = spark.createDataFrame(tuplas, schema) 
        df_por.show()
        grafico_sectores('Porcentaje por cada Grupo de edad durante ',age,i[1],i[0])

def buscar_media_total(df1):
    seasons = ['winter','spring','summer','autumn']
    lista = []
    for i in range(4) :
        df= df1.filter(df1['season']==seasons[i])
        media = buscar_media_total1(df,'travel_time')
        print('La media de tiempo de un viaje durante', seasons[i], 
              'es', media, 'segundos o', round(media/60,2), 'minutos' )
        lista.append([seasons[i],media])   
    return lista

def buscar_media_total1(df,var): 
    total = df.rdd.map(lambda x: x[f'{var}']).sum()
    long = df.count()
    media = total/long
    return media

def grafico_sectores(title,x,y,estacion):
    curses.wrapper(mostrar_grafica, title,'s', x, y,'et1','et2',estacion)
    
def grafico_barras(title,x,y,et1,et2):
    curses.wrapper(mostrar_grafica, title,'b', x, y, et1, et2,'estacion')
 
def mostrar_grafica(stdscr, title,cual, x, y, et1, et2,estacion):
    # Configurar el entorno de curses
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
        plt.title(title+estacion)
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
    
def main(files):
    
    dfs = []
    for f in files:
        dfs.append(spark.read.json(f))
        
    dfs1 = columnas(dfs)
    df_unionPre = union(dfs1)
    
    ## ESTUDIO GENERALIZADO ##
    
    # Limpieza de datos
    df_union = df_unionPre.na.drop(subset=['ageRange']) # quita valores null en age
    df_union = df_union.filter(df_union['travel_time']>=0)
    # 1. RANGO DE EDAD
    print('### RANGO DE EDAD DE CADA USUARIO DE BICIMAD ###')

    dac,age,age_count_use, age_count_use_max, age_count_use_min, mxa, mna = count_var(df_union,'ageRange')
    resultados_descriptivo("Grupo de Edad", dac, age, age_count_use, age_count_use_max, age_count_use_min, mxa, mna)
    grafico_barras("Veces por Grupo de Edad",age,age_count_use, "Grupo de Edad", "veces")    
    
    #1.1 PORCENTAJE COUNT EN FUNCION DE LA EDAD Y LA ESTACION 
    list_porc = porcent_count_age(df_union)
    df_show(list_porc)
    
    #1.2 MEDIA DE TIEMPO DE LOS VIAJES EN FUNCION DE LA ESTACIÓN
    list_medias=buscar_media_total(df_union)
    
    #2. ESTACIONES ANUALES
    print('### ESTACIONALIDAD ANUAL ###')

    dsc,season,season_count_use, season_count_use_max, season_count_use_min, mxs, mns = count_var(df_union,'season')
    resultados_descriptivo("Estaciones anuales", dsc,season,season_count_use, season_count_use_max, season_count_use_min, mxs, mns)
    grafico_barras("Veces por Estaciones anuales",season,season_count_use, "Estaciones", "veces")   
     
    #3. MESES
    print('### MESES ###')

    dmc,month,month_count_use, month_count_use_max, month_count_use_min, mxm, mnm = count_var(df_union,'month')
    resultados_descriptivo("Meses", dmc,month,month_count_use, month_count_use_max, month_count_use_min, mxm, mnm)
    grafico_barras("Veces por Mes",month,month_count_use, "Meses", "veces")   
 

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    main(FILES)
    spark.stop()
  
    