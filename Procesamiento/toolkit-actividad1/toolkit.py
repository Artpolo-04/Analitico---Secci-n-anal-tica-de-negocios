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
    columnas_no_numericas = []

    for columna in df.columns:
        # Validar si todos los valores son numéricos
        no_numericos = df[~df[columna].apply(lambda x: isinstance(x, (int, float)) or (isinstance(x, str) and x.replace(',', '').replace('.', '').isdigit()))]
        
        if not no_numericos.empty:
            columnas_no_numericas.append(columna)

    if columnas_no_numericas:
        print(f"⚠️ Alerta: Las siguientes columnas contienen valores no numéricos: {', '.join(columnas_no_numericas)}")
    else:
        print("✅ Todas las columnas son numéricas.")




def separar_datos_publico_objetivo(df_datos, df_rubros):
    """
    Realiza un join entre df_datos y df_rubros utilizando la columna 'cod_rubro' y separa las coincidencias en dos DataFrames: df_gcar y df_tc.

    Parámetros:
        df_datos (DataFrame): DataFrame con los datos principales.
        df_rubros (DataFrame): DataFrame con las columnas 'cod_rubro' y 'descripcion'.

    Retorna:
        df_gcar (DataFrame): DataFrame con las coincidencias para GCAR.
        df_tc (DataFrame): DataFrame con las coincidencias para TC.
    """
    df_datos.columns = df_datos.columns.astype(str)

    # Realizar el join entre df_datos y df_rubros
    df_joined = pd.merge(df_datos, df_rubros, on='cod_rubro', how='inner')

    # Separar las coincidencias en dos DataFrames diferentes
    df_gcar = df_joined[df_joined['descri_rubro'] == 'GCAR']
    df_tc = df_joined[df_joined['descri_rubro'] == 'Tamaño comercial']

    df_gcar.head()

    df_tc.head()

    return df_gcar, df_tc              

def calcular_crecimiento_porcentual(df):
    """
    Calcula el promedio anual para cada registro en el DataFrame y el crecimiento porcentual entre 2022 y 2023.

    Parámetros:
        df (DataFrame): DataFrame con columnas de meses en formato 'YYYYMM'.

    Retorna:
        DataFrame: DataFrame con columnas adicionales para el promedio anual y el crecimiento porcentual.
    """
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


def crecimiento_zona_gcar(df_zonas, df_gcar):
    """
    Calcula el crecimiento porcentual de cada zona en su Tamaño Comercial entre 2022 y 2023.

    Parámetros:
        df_zonas (DataFrame): DataFrame con las columnas 'zona' y 'Desc'.
        df_gcar (DataFrame): DataFrame con columnas de meses en formato 'YYYYMM' y columnas 'gcar_2022' y 'gcar_2023'.

    Retorna:
        DataFrame: DataFrame con columnas adicionales 'gcar_por_zona_2022', 'gcar_por_zona_2023' y 'gcar_zona_porcentual'.
    """

    gcar_por_zona = df_gcar.groupby('zona')[['gcar_2022', 'gcar_2023']].sum().reset_index()

    # Calcular el crecimiento porcentual para cada zona
    gcar_por_zona['gcar_zona_porcentual'] = ((gcar_por_zona['gcar_2023'] - gcar_por_zona['gcar_2022']) / gcar_por_zona['gcar_2022']) * 100

    # Redondear el crecimiento porcentual a 2 decimales
    gcar_por_zona['gcar_zona_porcentual'] = gcar_por_zona['gcar_zona_porcentual'].round(2)

    # Unir con el DataFrame de zonas para obtener las descripciones
    df_resultado = pd.merge(df_zonas, gcar_por_zona, on='zona', how='left')

    return df_resultado

def buscar_crecimiento_zona(df, valor_buscar):
    """
    Busca en el df con el valor de la gcar entre 2022 y 2023 en la zona de Antioquia 1.

    Parámetros:
        df (DataFrame): DataFrame que contiene las columnas 'zona', 'Desc', 'gcar_2022', 'gcar_2023' y 'gcar_zona_porcentual'.
        valor_buscar (str): Antioquia 1.

    """

    coincidencia = df[df['Desc'].str.contains(valor_buscar, case=False, na=False)]
    crecimiento = coincidencia['gcar_zona_porcentual'].values[0]
    print("Actividad 1.2")
    print(f"El valor de la GCAR en la zona {valor_buscar} con un crecimiento de {crecimiento} %")

def obtener_zona_menor_gcar(df_zonas, df):
    """
    Encuentra la zona con el menor GCAR en el primer semestre de 2022.

    Parámetros:
        df_zonas (DataFrame): DataFrame con las columnas 'zona' y 'Desc'.
        df (DataFrame): DataFrame con columnas de meses en formato 'YYYYMM' y una columna 'zona'.

    Retorna:
        None: Imprime la zona con el menor GCAR en el primer semestre de 2022.
    """
    # Filtrar las columnas que empiezan por '2022' hasta el mes '202206'
    columnas_hasta_junio_2022 = [col for col in df.columns if col.startswith('2022') and int(col[4:]) <= 6]

    df['suma_hasta_junio_2022'] = df[columnas_hasta_junio_2022].sum(axis=1)
    gcar_por_zona = df.groupby('zona')['suma_hasta_junio_2022'].sum().reset_index()
        
    zona_menor_gcar = gcar_por_zona.loc[gcar_por_zona['suma_hasta_junio_2022'].idxmin()]

    zona_menor_gcar = pd.merge(zona_menor_gcar.to_frame().T, df_zonas, on='zona', how='left')

    print(f"La zona con el menor GCAR en el primer semestre de 2022 es: {zona_menor_gcar['Desc'].values[0]} con un GCAR de {zona_menor_gcar['suma_hasta_junio_2022'].values[0]}")

def obtener_zona_mayor_tc2023(df_zonas,df):
    """
    Encuentra la zona con el mayor Tamaño Comercial (TC) en 2023.

    Parámetros:
        df_zonas (DataFrame): DataFrame con las columnas 'zona' y 'Desc'.
        df (DataFrame): DataFrame con columnas de meses en formato 'YYYYMM' y una columna 'zona'.

    Retorna:
        None: Imprime la zona con el mayor Tamaño Comercial en 2023.
    """
    columnas_2023 = [col for col in df.columns if col.startswith('2023')]

    df['suma_2023'] = df[columnas_2023].sum(axis=1)
    df['promedio_tc_2023'] = df['suma_2023'] / 12
    tc_por_zona = df.groupby('zona')['promedio_tc_2023'].sum().reset_index()

    zona_mayor_tc = tc_por_zona.loc[tc_por_zona['promedio_tc_2023'].idxmax()]

    zona_mayor_tc = pd.merge(zona_mayor_tc.to_frame().T, df_zonas, on='zona', how='left')

    print(f"La zona con el mayor Tamaño Comercial en 2023 es: {zona_mayor_tc['Desc'].values[0]} con un TC promedio de {zona_mayor_tc['promedio_tc_2023'].values[0]}")
