# Prueba Tecnica Sección analítica de negocios 🧑‍💻
Este repositorio ha sido realizado con el proposito de 
alojar los ejecutables enfocados a resolver la asignación de retos comerciales para la variable de GCAR del negocio empresarial por gerente comercial y analisis de tamaño comercial

## Requisitos de instalación 📝
Debido a la actividad numero 4 que se han solicitado las sentencias SQL que den respuesta a los numerales alli expresados, se ha decidido replicar la base de datos que alli se hace referencia con la libreria Pyspark ya que si en caso tal se desea ejecutar sera necesario tener esta libreria instalada.

A continuacion se hace referencia a las librerias utilizadas en este proyecto

* Python 💻 : Lenguaje de programación principal.
* Pandas 🐼: Manipulación y análisis de datos.
* PySpark 🕵️‍♂️ : Consultas SQL.
* Matplotlib 📈 : Visualización de datos.
* OpenPyXL 👀: Lectura y escritura de archivos Excel.
* Findspark 🔎: Inicialización de PySpark en el entorno de Python.

## Arquitectura del proyecto 
├── 📂 Insumos
│ ├── 📄 Prueba Bancolombia 2024 (Analitico).xlsx
├── 📂 Procesamiento
│ ├── 📂 Main
│ ├── ├── 🐍 main.py
│ ├── ├── ⚙️ params.json
│ ├── 📂 toolkit-actividad1
│ ├── ├── 🐍 toolkit.py
│ ├── 📂 toolkit-actividad2
│ ├── ├── 🐍 toolkit_act2.py
│ ├── 📂 toolkit-actividad4
│ ├── ├── 🐍 toolkit_act4.py
├── 📂 Resultados
│ ├── 📂 Actividad 2
│ ├── ├── 📈 comparacion_gcar_top5.PNG
│ ├── ├── 📝 Retos_2024.xlsx
│ ├── ├── 📝 Word Explicativo Act 2.docx
│ ├── 📂 Actividad 3
│ ├── ├── 📝 GCAR_por_zonas.xlsx
│ ├── ├── 📝 TC_por_zonas.xlsx

1. **Insumos:** Archivo Excel: `Insumos/Prueba Bancolombia 2024 (Analitico).xlsx`
2. **Procesamiento:** Los scripts en esta carpeta realizan el procesamiento de datos, análisis y generación de informes.

* Subcarpetas:
📂 toolkit-actividad1: Contiene funciones para la actividad 1.
📂 toolkit-actividad2: Contiene funciones para la actividad 2.
📂 toolkit-actividad3: Contiene funciones para la actividad 3 (Desarrollo del GCAR y TC por zonas)
📂 toolkit-actividad4: Contiene funciones para la actividad 4.
📂Archivo de parámetros: params.json

3. **Resultados:** Los resultados del procesamiento de la actividad 2 como los Retos_2024 y el documento .word explicativo

## Flujo de trabajo
🐍 Main.py es el Script principal del cual se coordina la ejecucion de la Actividad 1 2 3 y 4