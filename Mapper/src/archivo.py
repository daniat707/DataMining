import os

# Nombre del archivo original y del archivo de salida
input_file = "/Users/alexperez/Documents/DataMining/Mapper/src/Input.txt"
output_file = "input.txt"

# Tamaño deseado en bytes (1 GB = 1024 * 1024 * 1024 bytes)
desired_size = 1 * 1024 * 1024 * 1024  # 1 GB

# Leer el contenido del archivo original
with open(input_file, 'r') as f:
    content = f.read()

# Calcular el tamaño del archivo original
current_size = os.path.getsize(input_file)

# Abrir el archivo de salida para escribir
with open(output_file, 'w') as f_out:
    # Escribir el contenido del archivo original en el archivo de salida
    f_out.write(content)
    
    # Seguir duplicando el contenido hasta alcanzar el tamaño deseado
    while current_size < desired_size:
        f_out.write(content)
        current_size += len(content)
        
        # Imprimir el progreso
        print(f"Tamaño actual: {current_size / (1024 * 1024)} MB")

print(f"El archivo '{output_file}' ha sido creado con un tamaño de {current_size / (1024 * 1024)} MB.")