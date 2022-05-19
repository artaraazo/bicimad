# -*- coding: utf-8 -*-
"""
Created on Tue May 17 17:46:09 2022

@author: marin
"""

from pyspark import SparkContext
import json
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from collections import Counter

sc = SparkContext()

def estudios(rdd_julio,rdd_diciembre):
    datos_diciembre=rdd_diciembre.map(get_data)
    datos_julio=rdd_julio.map(get_data)

    contador_edadsem = Counter(datos_diciembre.filter(lambda x : x[0]== "Entre semana").map(lambda x : x[3]).collect())
    print("Uso en funcion del rango de edad entre semana: ",contador_edadsem)
    contador_edadfin = Counter(datos_diciembre.filter(lambda x : x[0]== "Fin de semana").map(lambda x : x[3]).collect())
    print("Uso en funcion del rango de edad en fin de semana: ",contador_edadfin)
    contador_edad = Counter(datos_diciembre.map(lambda x : x[3]).collect()) #uso en función del rango de edad general
    print("Uso en funcion del rango de edad: ",contador_edad)
    
    contador_usuario_v = Counter(datos_julio.map(lambda x : x[2]).collect())
    print("Uso en funcion del tipo de usuario en verano: ", contador_usuario_v)
    contador_usuario_i = Counter(datos_diciembre.map(lambda x : x[2]).collect())
    print("Uso en funcion del tipo de usuario en invierno: ",contador_usuario_i)

    rdd_est0_transitadas=Counter(datos_diciembre.map(lambda x : (x[4][0])).collect())
    rdd_est1_transitadas=Counter(datos_diciembre.map(lambda x : (x[4][1])).collect())
    rdd_est_transitadas=rdd_est0_transitadas+rdd_est1_transitadas
    print("Estaciones más transitadas: ",rdd_est_transitadas.most_common()[0:5])
    print("Estaciones menos transitadas: ",rdd_est_transitadas.most_common()[-5:])
    
    rdd_semana_tmedio=datos_diciembre.filter(lambda x : x[0]== "Entre semana").map(lambda x: (x[3],x[1])).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
    print('Mostramos la lista de tiempo medio de uso segun la edad entre semana: ')
    print(rdd_semana_tmedio)
    ejes = crear_lista(list(rdd_semana_tmedio))
    matplotlib.pyplot.bar(ejes[0],ejes[1])
    matplotlib.pyplot.ylabel('Tiempo medio')
    matplotlib.pyplot.xlabel('Edad')
    plt.title('Tiempo medio de uso dependiendo de la edad entre semana')
    matplotlib.pyplot.show()
    
    rdd_finde_tmedio=datos_diciembre.filter(lambda x : x[0]== "Fin de semana").map(lambda x: (x[3],x[1])).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
    print('Mostramos la lista de tiempo medio de uso segun la edad en fin de semana: ')
    print(rdd_finde_tmedio)
    ejes = crear_lista(list(rdd_finde_tmedio))
    matplotlib.pyplot.bar(ejes[0],ejes[1])
    matplotlib.pyplot.ylabel('Media')
    matplotlib.pyplot.xlabel('Edad')
    plt.title('Tiempo medio de uso dependiendo de la edad en fin de semana')
    matplotlib.pyplot.show()
    
    
    
def crear_lista(lista):
    result1 = []
    result2 = []
    for i in lista:
        result1.append(i[0])
        result2.append(i[1])
    return [result1,result2]

def get_data(line): 
    data = json.loads(line)
    fecha=data['unplug_hourTime'].split('T')[0]
    tiempo_viaje = data['travel_time']
    tipo_usuario = data['user_type']
    rango_edad = data['ageRange']
    est_salida = data['idunplug_station']
    est_llegada = data['idplug_station']
    finde = pd.Timestamp(fecha)

    if finde.dayofweek <= 4:
        semana = "Entre semana" 
    else:
        semana = "Fin de semana" 

    return semana,tiempo_viaje,tipo_usuario,rango_edad, (est_salida,est_llegada)


def main(sc):
    rdd_julio = sc.parallelize([])
    rdd_julio=rdd_julio.union(sc.textFile("201907_movements.json"))
    rdd_diciembre = sc.parallelize([])
    rdd_diciembre=rdd_diciembre.union(sc.textFile("201912_movements.json"))
    estudios(rdd_julio,rdd_diciembre)


if __name__ =="__main__":
    main(sc)
    