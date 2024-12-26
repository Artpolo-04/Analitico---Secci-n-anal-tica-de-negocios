import pandas as pd
import findspark
findspark.init()

from pyspark.sql import SparkSession

def crear_datafreams():

    spark = SparkSession.builder.appName("CrearDataFrames").getOrCreate()

    # TblConceptoDetalle
    TblConceptoDetalle = pd.DataFrame({
        "IdDetalleConcepto": [1, 2, 3],
        "IdConcepto": [10, 11, 12],
        "NombreConcepto": ["Concepto A", "Concepto B", "Concepto C"],
        "Descripcion": ["Detalle A", "Detalle B", "Detalle C"],
        "Activo": [1, 1, 0]
    })

    # TblVendedor
    TblVendedor = pd.DataFrame({
        "Identificacion": [101, 102, 103],
        "TipoDeIdentificacion": [1, 2, 1],
        "Nombre1": ["Juan", "Ana", "Luis"],
        "Nombre2": ["Carlos", None, "Miguel"],
        "Apellido1": ["Perez", "Lopez", "Martinez"],
        "Apellido2": ["Diaz", "Garcia", "Hernandez"],
        "Direccion": ["Calle A", "Calle B", "Calle C"],
        "Telefono": ["123456789", "987654321", "456123789"],
        "EstadoCivil": [1, 2, 1],
        "Sexo": [1, 2, 1],
        "Activo": [1, 0, 1]
    })

    # TblVenta
    TblVenta = pd.DataFrame({
        "IdFactura": [1001, 1002, 1003],
        "Vendedor": [101, 102, 103],
        "Fechas": ["2023-12-01", "2023-12-02", "2023-12-03"],
        "Iva": [18, 20, 15],
        "IdTienda": [1, 2, 3]
    })

    # TblDetalleVenta
    TblDetalleVenta = pd.DataFrame({
        "IdDetalleFactura": [1, 2, 3],
        "IdFactura": [1001, 1001, 1002],
        "IdProducto": [2001, 2002, 2003],
        "Cantidad": [2, 1, 3],
        "Total": [200, 100, 300]
    })

    # TblProducto
    TblProducto = pd.DataFrame({
        "IdProducto": [2001, 2002, 2003],
        "Nombre": ["Producto A", "Producto B", "Producto C"],
        "Descripcion": ["Desc A", "Desc B", "Desc C"],
        "IdCategoria": [1, 2, 3],
        "Imagen": ["No aplica", "No aplica", "No aplica"],
        "Precio": [100, 50, 75],
        "UnidadDeMedida": ["Pieza", "Kg", "Litro"],
        "UnidaddxMedida": [1, 1, 1],
        "StockMinimo": [5, 10, 2],
        "StockMaximo": [50, 100, 20]
    })

    # TblCategoria
    TblCategoria = pd.DataFrame({
        "IdCategoria": [1, 2, 3],
        "Descripcion": ["Categoria A", "Categoria B", "Categoria C"],
        "Activo": [1, 1, 0]
    })

    # TblConcepto
    TblConcepto = pd.DataFrame({
        "IdConcepto": [10, 11, 12],
        "NombreConcepto": ["General", "Descuento", "Impuesto"],
        "Descripcion": ["General Desc", "Discount Desc", "Tax Desc"],
        "Activo": [1, 1, 0]
    })

    # Convertir los DataFrames de pandas a DataFrames de Spark y crear vistas temporales
    spark_tbl_concepto_detalle = spark.createDataFrame(TblConceptoDetalle)
    spark_tbl_concepto_detalle.createOrReplaceTempView("TblConceptoDetalle")

    spark_tbl_vendedor = spark.createDataFrame(TblVendedor)
    spark_tbl_vendedor.createOrReplaceTempView("TblVendedor")

    spark_tbl_venta = spark.createDataFrame(TblVenta)
    spark_tbl_venta.createOrReplaceTempView("TblVenta")

    spark_tbl_detalle_venta = spark.createDataFrame(TblDetalleVenta)
    spark_tbl_detalle_venta.createOrReplaceTempView("TblDetalleVenta")

    spark_tbl_producto = spark.createDataFrame(TblProducto)
    spark_tbl_producto.createOrReplaceTempView("TblProducto")

    spark_tbl_categoria = spark.createDataFrame(TblCategoria)
    spark_tbl_categoria.createOrReplaceTempView("TblCategoria")

    spark_tbl_concepto = spark.createDataFrame(TblConcepto)
    spark_tbl_concepto.createOrReplaceTempView("TblConcepto")

    print("Vistas temporales creadas en Spark:")
    print("TblConceptoDetalle, TblVendedor, TblVenta, TblDetalleVenta, TblProducto, TblCategoria, TblConcepto")

    return spark

def consultasActividad4(spark):
    print("""1. Cuáles son las ventas de cada uno de los productos vendidos por categoría
            y por cada uno de los vendedores, indique aquí los nombres de estado civil 
            sexo y tipo de identificación de cada vendedor en la consulta  """)
    spark.sql("""SELECT 
                c.Descripcion AS Categoria,
                p.Nombre AS Producto,
                SUM(dv.Cantidad) AS CantidadVendida,
                SUM(dv.Total) AS TotalVendido,
                v.Nombre1 AS NombreVendedor,
                v.Apellido1 AS ApellidoVendedor,
                v.EstadoCivil,
                v.Sexo,
                v.TipoDeIdentificacion
            FROM 
                TblDetalleVenta dv
            INNER JOIN 
                TblVenta tv ON dv.IdFactura = tv.IdFactura
            INNER JOIN 
                TblProducto p ON dv.IdProducto = p.IdProducto
            INNER JOIN 
                TblCategoria c ON p.IdCategoria = c.IdCategoria
            INNER JOIN 
                TblVendedor v ON tv.Vendedor = v.Identificacion
            GROUP BY 
                c.Descripcion, 
                p.Nombre, 
                v.Nombre1, 
                v.Apellido1, 
                v.EstadoCivil, 
                v.Sexo, 
                v.TipoDeIdentificacion
            ORDER BY 
                Categoria, 
                NombreVendedor, 
                Producto""").show()
    print("""
    2. Cuáles son los productos que han tenido mayor venta y a qué vendedor pertenece?
        """)
    spark.sql("""
            SELECT 
                p.Nombre AS Producto,
                SUM(dv.Cantidad) AS CantidadVendida,
                SUM(dv.Total) AS TotalVendido,
                v.Nombre1 AS NombreVendedor,
                v.Apellido1 AS ApellidoVendedor
            FROM 
                TblDetalleVenta dv
            INNER JOIN 
                TblVenta tv ON dv.IdFactura = tv.IdFactura
            INNER JOIN 
                TblProducto p ON dv.IdProducto = p.IdProducto
            INNER JOIN 
                TblVendedor v ON tv.Vendedor = v.Identificacion
            GROUP BY 
                p.Nombre, 
                v.Nombre1, 
                v.Apellido1
            ORDER BY 
                TotalVendido DESC
            LIMIT 1

    """).show()
    print("3.resumen general")
    spark.sql("""
                SELECT 
                    c.Descripcion AS Categoria,
                    p.Nombre AS Producto,
                    SUM(dv.Cantidad) AS CantidadVendida,
                    SUM(dv.Total) AS TotalVendido,
                    v.Nombre1 AS NombreVendedor,
                    v.Apellido1 AS ApellidoVendedor,
                    v.TipoDeIdentificacion,
                    v.EstadoCivil,
                    v.Sexo,
                    cd.NombreConcepto AS ConceptoDetalle,
                    cd.Descripcion AS DetalleDescripcion,
                    co.NombreConcepto AS Concepto,
                    co.Descripcion AS ConceptoDescripcion
                FROM 
                    TblDetalleVenta dv
                INNER JOIN 
                    TblVenta tv ON dv.IdFactura = tv.IdFactura
                INNER JOIN 
                    TblProducto p ON dv.IdProducto = p.IdProducto
                INNER JOIN 
                    TblCategoria c ON p.IdCategoria = c.IdCategoria
                INNER JOIN 
                    TblVendedor v ON tv.Vendedor = v.Identificacion
                INNER JOIN 
                    TblConceptoDetalle cd ON cd.IdDetalleConcepto = p.IdProducto
                INNER JOIN 
                    TblConcepto co ON co.IdConcepto = cd.IdConcepto
                GROUP BY 
                    c.Descripcion, 
                    p.Nombre, 
                    v.Nombre1, 
                    v.Apellido1, 
                    v.TipoDeIdentificacion, 
                    v.EstadoCivil, 
                    v.Sexo, 
                    cd.NombreConcepto, 
                    cd.Descripcion, 
                    co.NombreConcepto, 
                    co.Descripcion
                ORDER BY 
                    Categoria, 
                    Producto, 
                    TotalVendido DESC
            """).show()
