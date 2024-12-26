import matplotlib.pyplot as plt
import pandas as pd 
import os


def calculo_reto_gcar(df_gcar_each_year,porcentaje_crecimiento):
    """
    Calcula el reto de crecimiento para el Tamaño Comercial de cada zona en 2023.

    Parámetros:
        df_gcar_each_year (DataFrame): DataFrame con columnas 'gcar_2022' y 'gcar_2023'.
        porcentaje_crecimiento (float): Porcentaje de crecimiento esperado.

    Retorna:
        El df con la columna 'reto_gcar' que contiene el reto de crecimiento para el año 2024.
    """
    df_gcar_each_year['reto_gcar'] = (df_gcar_each_year['gcar_2023'] * (1 + porcentaje_crecimiento)).round(3)   
    df_seleccionado = df_gcar_each_year[['zona', 'gerente', 'cod_rubro', 'reto_gcar']]
    print(df_seleccionado)    
    return df_gcar_each_year

def graficar_top5_gcar(df_gcar_reto):
    """
    Crea una gráfica de barras que compara gcar_2022, gcar_2023 y reto_gcar para los 5 gerentes con mayor valor en reto_gcar y la guarda en un archivo.

    Parámetros:
        df_gcar_reto (DataFrame): DataFrame con columnas 'gerente', 'gcar_2022', 'gcar_2023' y 'reto_gcar'.

    Retorna:
        Guarda la grafica de barras en la carpeta Resultados/Actividad2.
    """
    df_seleccionado = df_gcar_reto[['gerente', 'gcar_2022', 'gcar_2023', 'reto_gcar']]

    df_top5 = df_seleccionado.nlargest(5, 'reto_gcar')

    plt.figure(figsize=(14, 8))

    df_top5.set_index('gerente').plot(kind='bar')

    plt.title('''Comparación de GCAR 2022, GCAR 2023 y Reto GCAR 
              por los 5 Gerentes con Mayor Valor en Reto GCAR''')
    plt.xlabel('Gerente')
    plt.ylabel('Valor GCAR')
    plt.xticks(rotation=45, ha='right')
    plt.legend()

    output_dir = "Resultados/Actividad2"
    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(output_dir, "comparacion_gcar_top5.png")
    plt.savefig(output_file)

    print(f"Gráfica guardada en {output_file}")

def calculo_gcar_proporcionalidad(df,crecimiento):
    """
    Calcula el peso del gerente y el reto del gerente basado en el GCAR de 2023 y guarda los resultados en un archivo Excel.

    Parámetros:
        df (DataFrame): DataFrame con columnas 'zona', 'gerente', 'cod_rubro', 'gcar_2023'.

    Retorna:
        None
    """
    porcentaje_crecimiento = float(crecimiento) / 100 + 1

    gcar_total_2023 = df['gcar_2023'].sum()

    df['peso_gerente'] = df['gcar_2023'] / gcar_total_2023

    gcar_total_2024 = gcar_total_2023 * porcentaje_crecimiento

    df['reto_gerente'] = df['peso_gerente'] * gcar_total_2024

    df_resultado = df[['zona', 'gerente', 'cod_rubro', 'gcar_2023', 'peso_gerente', 'reto_gerente']]

    # Guardar los resultados en un archivo Excel
    df_resultado.to_excel("Resultados/Retos_2024.xlsx", index=False)
    print("Resultados guardados en Resultados/Retos_2024.xlsx")
