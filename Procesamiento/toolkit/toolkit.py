'''En esta toolkit se encontraran las funciones que han sido 
utilizadas en el desarrollo de los notebooks de la carpeta Procesamiento.'''


import json
import pandas as pd


def cargar_parametros_json(filepath):
    """
    Carga parámetros desde un archivo JSON.

    Parámetros:
        filepath (str): Ruta del archivo JSON.

    Retorna:
        dict: Diccionario con los parámetros cargados desde el archivo JSON.
    """
    with open(filepath, 'r') as file:
        parametros = json.load(file)
    return parametros


def get_data(path, sheet_name=None):
    """
    Función para obtener datos de un archivo Excel y llama funcion que 
    valida que todas las columnas sean numéricas.
    
    Parámetros:
        - path (str): Ruta del archivo Excel.
        - sheet_name (str): Nombre de la hoja que se quiere leer. 
                            Si es None, se leerá la primera hoja.
    
    Retorna:
        - DataFrame de pandas con los datos de la hoja seleccionada.
    """

    print(f'Obteniendo datos desde el archivo: {path}')
    if sheet_name:
        print(f'Leyendo la hoja: {sheet_name}')
    else:
        print('Leyendo la primera hoja del archivo.')
    
    try:
        data = pd.read_excel(path, sheet_name=sheet_name)
        print(f'Datos obtenidos correctamente. Dimensiones: {data.shape}')
        return data
    except Exception as e:
        print(f'Error al leer el archivo: {e}')
        return None

def validar_columnas_numericas(df):
    """
    Valida si todas las columnas de un DataFrame contienen únicamente datos numéricos.
    Si encuentra valores no numéricos, imprime una alerta con los detalles.

    :param df: DataFrame de pandas a validar
    """
    print("Validando columnas numéricas...")
    for columna in df.columns:
        # Validar si todos los valores son numéricos
        no_numericos = df[~df[columna].apply(lambda x: isinstance(x, (int, float)) or (isinstance(x, str) and x.replace(',', '').replace('.', '').isdigit()))]
        
        if not no_numericos.empty:
            print(f"⚠️ Alerta: La columna '{columna}' contiene valores no numéricos:")
            print(no_numericos[columna].unique())
        else:
            print(f"✅ La columna '{columna}' es numérica.")

def calcular_crecimiento_porcentual(df):
    """
    Calcula el promedio anual para cada registro en el DataFrame y el crecimiento porcentual entre 2022 y 2023.

    Parámetros:
        df (DataFrame): DataFrame con columnas de meses en formato 'YYYYMM'.

    Retorna:
        DataFrame: DataFrame con columnas adicionales para el promedio anual y el crecimiento porcentual.
    """
    # Convertir los nombres de las columnas a cadenas de texto
    df.columns = df.columns.map(str)

    # Filtra columnas que son cadenas de texto y empiezan con '2022' o '2023'
    columnas_2022 = [col for col in df.columns if col.startswith('2022')]
    columnas_2023 = [col for col in df.columns if col.startswith('2023')]

    df['Promedio_2022'] = df[columnas_2022].mean(axis=1)
    df['Promedio_2023'] = df[columnas_2023].mean(axis=1)

    # Calcular el crecimiento porcentual entre 2022 y 2023
    df['Crecimiento'] = ((df['Promedio_2023'] - df['Promedio_2022']) / df['Promedio_2022']) * 100

    return df

def gerente_mayor_crecimiento(df):
    """
    Encuentra el gerente con el mayor crecimiento porcentual entre 2022 y 2023.

    Parámetros:
        df (DataFrame): DataFrame con columnas de meses en formato 'YYYYMM' y columnas adicionales de promedios y crecimiento.

    Imprime:
        Nombre del gerente con mayor crecimiento y el valor del crecimiento porcentual.
    """
    df_crecimiento = calcular_crecimiento_porcentual(df)
    idx_max_crecimiento = df_crecimiento['Crecimiento'].idxmax()
    nombre_gerente = df_crecimiento.loc[idx_max_crecimiento, 'gerente']
    cod_rubro = df_crecimiento.loc[idx_max_crecimiento, 'cod_rubro']
    crecimiento = df_crecimiento.loc[idx_max_crecimiento, 'Crecimiento']
    print("----------------------------------------------------------------------------------------------------")
    print("Actividad 1:")
    print(f"El gerente con mayor crecimiento en su Tamaño Comercial entre 2022 y 2023 es: {nombre_gerente} de codigo {cod_rubro} con un crecimiento del {crecimiento:.2f}%")

def gcar_each_year(df):
    """
    Suma los valores de cada columna correspondiente a cada año y agrega nuevas columnas 'gcar_2022' y 'gcar_2023'.

    Parámetros:
        df (DataFrame): DataFrame con columnas de meses en formato 'YYYYMM'.

    Retorna: 
        DataFrame: DataFrame con las columnas 'gcar_2022' y 'gcar_2023' que contienen la suma de los valores de cada año.
    """
    columnas_2022 = [col for col in df.columns if col.startswith('2022')]
    columnas_2023 = [col for col in df.columns if col.startswith('2023')]
    df['gcar_2022'] = df[columnas_2022].sum(axis=1)
    df['gcar_2023'] = df[columnas_2023].sum(axis=1)
    return df

def cod_zona_buscar(df_zonas, nombre_buscar):
    """
    Busca el código de una zona por una coincidencia parcial en su descripción.

    Parámetros:
        df_zonas (DataFrame): DataFrame con las columnas 'zona' y 'Desc'.
        nombre_buscar (str): Parte del nombre de la zona a buscar.

    Retorna:
        str: Código de la zona si se encuentra, None si no se encuentra.
    """
    coincidencias = df_zonas[df_zonas['Desc'].str.contains(nombre_buscar, case=False, na=False)]
    
    if coincidencias.empty:
        print(f"No se encontraron coincidencias para '{nombre_buscar}' en la columna 'Desc'.")
        return None
        
    return coincidencias.iloc[0]['zona']

def crecimiento_zona_gcar(df_gcar,cod_zona):
    """
    Calcula el crecimiento porcentual de la zona en su Tamaño Comercial entre 2022 y 2023.

    Parámetros:
        df_gcar (DataFrame): DataFrame con columnas de meses en formato 'YYYYMM' y columnas 'gcar_2022' y 'gcar_2023'.
        cod_zona (str): Código de la zona a calcular el crecimiento.

    Retorna:
        float: Crecimiento porcentual de la zona en su Tamaño Comercial entre 2022 y 2023.
    """
    gcar_2022_zona = df_gcar[df_gcar['zona'] == cod_zona]['gcar_2022'].sum()
    gcar_2023_zona = df_gcar[df_gcar['zona'] == cod_zona]['gcar_2023'].sum()
    
    return round(((gcar_2023_zona - gcar_2022_zona) / gcar_2022_zona) * 100, 2)