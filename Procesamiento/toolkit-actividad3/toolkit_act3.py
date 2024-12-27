import pandas as pd

def calcular_gcar_por_zonas(df_zonas, df):
    """
    Calcula el GCAR por zonas para cada año y guarda el resultado en un archivo Excel.

    Parámetros:
        df_zonas (DataFrame): DataFrame con las columnas 'zona' y 'Desc'.
        df (DataFrame): DataFrame con columnas de meses en formato 'YYYYMM' y una columna 'zona'.
        output_file (str): Ruta del archivo Excel donde se guardará el resultado.

    Retorna:
        None
    """
    columnas_2022 = [col for col in df.columns if col.startswith('2022')]
    columnas_2023 = [col for col in df.columns if col.startswith('2023')]

    df['gcar_2022'] = df[columnas_2022].sum(axis=1)
    df['gcar_2023'] = df[columnas_2023].sum(axis=1)

    gcar_por_zona_2022 = df.groupby('zona')['gcar_2022'].sum().reset_index()
    gcar_por_zona_2023 = df.groupby('zona')['gcar_2023'].sum().reset_index()

    gcar_por_zona_2022 = pd.merge(gcar_por_zona_2022, df_zonas, on='zona', how='left')
    gcar_por_zona_2023 = pd.merge(gcar_por_zona_2023, df_zonas, on='zona', how='left')

    gcar_por_zona_2022.rename(columns={'gcar_2022': 'GCAR 2022'}, inplace=True)
    gcar_por_zona_2023.rename(columns={'gcar_2023': 'GCAR 2023'}, inplace=True)

    gcar_por_zonas = pd.merge(gcar_por_zona_2022, gcar_por_zona_2023, on=['zona', 'Desc'], how='outer')

    gcar_por_zonas.to_excel('Resultados/Actividad3/GCAR_por_zonas.xlsx', index=False)
    print(f"El GCAR por zonas para cada año se ha guardado en 'Resultados/Actividad3/GCAR_por_zonas.xlsx'")


def calcular_tc_por_zonas(df_zonas, df):
    """
    Calcula el Tamaño Comercial (TC) por zonas para cada año y guarda el resultado en un archivo Excel.

    Parámetros:
        df_zonas (DataFrame): DataFrame con las columnas 'zona' y 'Desc'.
        df (DataFrame): DataFrame con columnas de meses en formato 'YYYYMM' y una columna 'zona'.
        output_file (str): Ruta del archivo Excel donde se guardará el resultado.

    Retorna:
        None
    """
    columnas_2022 = [col for col in df.columns if col.startswith('2022')]
    columnas_2023 = [col for col in df.columns if col.startswith('2023')]

    df['tc_2022'] = df[columnas_2022].mean(axis=1)
    df['tc_2023'] = df[columnas_2023].mean(axis=1)

    # Agrupar por zona y sumar el TC anual
    tc_por_zona_2022 = df.groupby('zona')['tc_2022'].sum().reset_index()
    tc_por_zona_2023 = df.groupby('zona')['tc_2023'].sum().reset_index()

    # Unir los resultados con el DataFrame de zonas
    tc_por_zona_2022 = pd.merge(tc_por_zona_2022, df_zonas, on='zona', how='left')
    tc_por_zona_2023 = pd.merge(tc_por_zona_2023, df_zonas, on='zona', how='left')

    # Renombrar las columnas para claridad
    tc_por_zona_2022.rename(columns={'tc_2022': 'TC 2022'}, inplace=True)
    tc_por_zona_2023.rename(columns={'tc_2023': 'TC 2023'}, inplace=True)

    # Unir los resultados de ambos años
    tc_por_zonas = pd.merge(tc_por_zona_2022, tc_por_zona_2023, on=['zona', 'Desc'], how='outer')

    # Guardar el resultado en un archivo Excel
    tc_por_zonas.to_excel('Resultados\Actividad3/TC_por_zonas.xlsx', index=False)
    print(f"El Tamaño Comercial (TC) por zonas para cada año se ha guardado en 'Resultados\Actividad3'")

