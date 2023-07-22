import mysql.connector
# Establecer la conexión con la base de datos
conexion = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="floaton92JI",
    database="example_db"
)
 # Crear un cursor para ejecutar consultas
cursor = conexion.cursor()
 # Ejecutar la consulta de inserción
consulta = "INSERT INTO usuarios (nombre, correo) VALUES (%s, %s)"
valores = ("valor1", "valor2")
cursor.execute(consulta, valores)
 # Confirmar los cambios en la base de datos
conexion.commit()
 # Cerrar el cursor y la conexión
cursor.close()
conexion.close()