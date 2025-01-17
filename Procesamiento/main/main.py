import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'toolkit-actividad1')))
from toolkit import cargar_parametros_json,get_data, validar_columnas_numericas, separar_datos_publico_objetivo
from toolkit import gerente_mayor_crecimiento,gcar_each_year,crecimiento_zona_gcar,buscar_crecimiento_zona, obtener_zona_menor_gcar, obtener_zona_mayor_tc2023

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'toolkit-actividad2')))
from toolkit_act2 import calculo_reto_gcar, graficar_top5_gcar, calculo_gcar_proporcionalidad

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'toolkit-actividad3')))
from toolkit_act3 import calcular_gcar_por_zonas, calcular_tc_por_zonas

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'toolkit-actividad4')))
from toolkit_act4 import crear_datafreams, consultasActividad4

params = "Procesamiento\main\params.json"

def ejecucionAct1Act2():
    parametros = cargar_parametros_json(params)
    #Actividad 1.1 
    datos = get_data(parametros.get('ruta_datos'), sheet_name="Datos")
    validar_columnas_numericas(datos)
    df_rubros = get_data(parametros.get('ruta_datos'), sheet_name="Rubros codigos")
    df_gcar, df_tc = separar_datos_publico_objetivo(datos,df_rubros)

    #Gerente con mayor crecimiento en su TC entre 2022 y 2023
    gerente_mayor_crecimiento(df_tc)

    #Actividad 1.2
    df_gcar_each_year=gcar_each_year(df_gcar)

    df_zona_codigos = get_data(parametros.get('ruta_datos'), sheet_name="Zonas códigos")
    nombre_buscar="antioquia 1"
    df_crecimiento= crecimiento_zona_gcar(df_zona_codigos, df_gcar_each_year)
    buscar_crecimiento_zona(df_crecimiento, nombre_buscar)

    #Actividad 1.3
    print("Actividad 1.3")
    obtener_zona_menor_gcar(df_zona_codigos, df_gcar)
    obtener_zona_mayor_tc2023(df_zona_codigos,df_tc)

    # Submenú para la Actividad 2
    punto2 = input("Desea continuar con la actividad 2? (si/no): ").strip().lower()
    if punto2 == "si":
        df_gcar2024 = calculo_reto_gcar(df_gcar_each_year, parametros.get('porcetaje_crecimiento'))
        graficar_top5_gcar(df_gcar2024)
        calculo_gcar_proporcionalidad(df_gcar_each_year,parametros.get('porcetaje_crecimiento'))
    else:
        print("Actividad 2 omitida.")

    punto3 = input("Desea continuar con la actividad 3? (si/no): ").strip().lower()
    if punto3 == "si":
        calcular_gcar_por_zonas(df_zona_codigos, df_gcar)
        calcular_tc_por_zonas(df_zona_codigos,df_tc)

    else:
        print("Actividad 2 omitida.")


def actividad4():
    spark = crear_datafreams()
    consultasActividad4(spark)

if __name__ == "__main__":
    while True:
        print("Menú de Actividades:")
        print("1. Ejecutar Actividad 1, 2 y 3")
        print("4. Ejecutar Actividad 4")
        print("0. Salir")
        
        opcion = input("Seleccione una opción: ")
        
        if opcion == "1":
            ejecucionAct1Act2()
        elif opcion == "4":
            actividad4()
        elif opcion == "0":
            print("Saliendo del programa...")
            break
        else:
            print("Opción no válida. Por favor, intente de nuevo.")