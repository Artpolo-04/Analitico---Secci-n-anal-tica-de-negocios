import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'toolkit')))
from toolkit import cargar_parametros_json,get_data, validar_columnas_numericas, gerente_mayor_crecimiento,gcar_each_year, cod_zona_buscar, crecimiento_zona_gcar

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
    cod_zona = cod_zona_buscar(df_zona_codigos, nombre_buscar)
    crecimiento_zona = crecimiento_zona_gcar(df_gcar_each_year,cod_zona)
    print("----------------------------------------------------------------------------------------------------")
    print(f"El crecimiento de la zona {nombre_buscar} con codigo {cod_zona} es: {crecimiento_zona}%")
