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
 
    print("3.resumen general")
    spark.sql("""
                SELECT 
                    v.Identificacion AS VendedorID,
                    CONCAT(v.Nombre1, ' ', COALESCE(v.Nombre2, ''), ' ', v.Apellido1, ' ', COALESCE(v.Apellido2, '')) AS VendedorNombre,
                    c.Descripcion AS Categoria,
                    p.Nombre AS Producto,
                    SUM(dv.Cantidad) AS TotalCantidad,
                    SUM(dv.Total) AS TotalVentas,
                    COUNT(DISTINCT dt.IdFactura) AS TotalFacturas
                FROM TblDetalleVenta dv
                INNER JOIN TblVenta dt ON dv.IdFactura = dt.IdFactura
                INNER JOIN TblVendedor v ON dt.Vendedor = v.Identificacion
                INNER JOIN TblProducto p ON dv.IdProducto = p.IdProducto
                INNER JOIN TblCategoria c ON p.IdCategoria = c.IdCategoria
                WHERE v.Activo = 1 AND c.Activo = 1
                GROUP BY v.Identificacion, CONCAT(v.Nombre1, ' ', COALESCE(v.Nombre2, ''), ' ', v.Apellido1, ' ', COALESCE(v.Apellido2, '')), c.Descripcion, p.Nombre
                ORDER BY v.Identificacion, TotalVentas DESC;

            """).show()
