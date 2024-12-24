import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'toolkit')))
from toolkit import cargar_parametros_json,get_data, validar_columnas_numericas
from toolkit import gerente_mayor_crecimiento,gcar_each_year,crecimiento_zona_gcar,buscar_crecimiento_zona, obtener_zona_menor_gcar, obtener_mayor_tc2023

params = "Procesamiento\main\params.json"


if __name__ == "__main__":
    parametros = cargar_parametros_json(params)
    #Actividad 1.1 
    datos = get_data(parametros.get('ruta_datos'), sheet_name="Datos")
    validar_columnas_numericas(datos)
    #Gerente con mayor crecimiento en su TC entre 2022 y 2023
    gerente_mayor_crecimiento(datos)
    #Actividad 1.2
    df_gcar_each_year=gcar_each_year(datos)

    df_zona_codigos = get_data(parametros.get('ruta_datos'), sheet_name="Zonas códigos")
    nombre_buscar="antioquia 1"
    df_crecimiento= crecimiento_zona_gcar(df_zona_codigos, df_gcar_each_year)
    buscar_crecimiento_zona(df_crecimiento, nombre_buscar)

    #Actividad 1.3
    print("Actividad 1.3")
    obtener_zona_menor_gcar(df_zona_codigos, datos)
    obtener_mayor_tc2023(df_zona_codigos,datos)