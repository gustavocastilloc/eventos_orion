import pandas as pd
import numpy as np
from datetime import datetime
import re
from datetime import timedelta
# Proveedores válidos
PROVEEDORES_VALIDOS = ['telconet', 'puntonet', 'cnt', 'movistar', 'cirion', 'claro', 'newaccess']

# 1. Cargar el archivo y eliminar las dos primeras filas
archivo = "files/Report_Testing_orion_ggcas.xlsx"
df = pd.read_excel(archivo, skiprows=2)


df["EventTime"] = pd.to_datetime(df["EventTime"]) - timedelta(hours=5)
df["EventTime"] = pd.to_datetime(df["EventTime"], errors='coerce')

# 3. Filtrar filas que tengan un proveedor válido (nombre del enlace debe contener uno de los proveedores)
def extraer_proveedor(enlace):
    for prov in PROVEEDORES_VALIDOS:
        if prov.lower() in enlace.lower():
            return prov.capitalize()
    return None

df["Proveedor"] = df["Message"].apply(extraer_proveedor)
df = df[df["Proveedor"].notnull()]  # Solo enlaces con proveedor válido

# 4. Extraer nombre base del enlace (sin proveedor al final)
def extraer_agencia_base(message):
    # Frases clave donde se debe cortar el mensaje
    frases_clave = [
        'has stopped responding',
        'rebooted',
        'is responding again'
    ]
    
    mensaje_limpio = message
    
    for frase in frases_clave:
        if frase in mensaje_limpio.lower():
            # Encontramos la posición de la frase (en minúscula) y cortamos desde ahí
            idx = mensaje_limpio.lower().find(frase)
            mensaje_limpio = mensaje_limpio[:idx].strip()
            break  # solo cortamos por la primera coincidencia
    
    return mensaje_limpio.strip()




df["Agencia_base"] = df["Message"].apply(extraer_agencia_base)

resultado = []

# Crear diccionario con todos los eventos reboot redondeados al minuto por Agencia_base
reboots_por_agencia = {}
for _, fila in df[df['EventTypeName'].str.lower().str.contains('reboot')].iterrows():
    agencia = fila['Agencia_base']
    fecha_reboot = fila['EventTime'].replace(second=0, microsecond=0)
    reboots_por_agencia.setdefault(agencia, set()).add(fecha_reboot)

# Función para buscar reboot dentro de +/- 2 minutos
def hay_reboot_cercano(agencia, fecha_up):
    if agencia not in reboots_por_agencia:
        return False
    fecha_up_red = fecha_up.replace(second=0, microsecond=0)
    ventanas = [
        fecha_up_red + timedelta(minutes=i)
        for i in range(-2, 3)  # -2, -1, 0, +1, +2
    ]
    return any(f in reboots_por_agencia[agencia] for f in ventanas)

# Agrupar por Agencia_base
for enlace, grupo in df.groupby('Agencia_base'):
    eventos = grupo.sort_values('EventTime')
    fecha_down = None
    fecha_up = None

    for _, fila in eventos.iterrows():
        evento = str(fila['EventTypeName']).lower()

        if 'down' in evento and fecha_down is None:
            fecha_down = fila['EventTime']
        elif 'up' in evento and fecha_down is not None:
            fecha_up = fila['EventTime']
            hay_reboot = hay_reboot_cercano(enlace, fecha_up)

            estado = 'Reboot' if hay_reboot else 'Caído y recuperado'
            tiempo = round((fecha_up - fecha_down).total_seconds() / 60)

            resultado.append({
                'Enlace': enlace,
                'Fecha Down': fecha_down,
                'Fecha Up': fecha_up,
                'Tiempo': tiempo,
                'Estado': estado,
                'Agencia_base': fila['Agencia_base'],
                'Proveedor': fila['Proveedor']
            })

            fecha_down = None  # Reiniciar para buscar más pares
            fecha_up = None

    # Al finalizar el grupo, si quedó una caída sin subida
    if fecha_down is not None and fecha_up is None:
        resultado.append({
            'Enlace': enlace,
            'Fecha Down': fecha_down,
            'Fecha Up': None,
            'Tiempo': None,
            'Estado': 'Caído',
            'Agencia_base': fila['Agencia_base'],
            'Proveedor': fila['Proveedor']
        })
# Convertimos el resultado a DataFrame
df_resultado = pd.DataFrame(resultado)

# Diccionario para enlaces principales con estado Reboot


def corregir_estados_reboot(df, time_margin='2min'):
    # Convertir el margen de tiempo a Timedelta
    time_margin = pd.Timedelta(time_margin)
    print("######### Corregir estados ##########")
    print(df.head(30))
    # Asegurar que las columnas de fecha son datetime, manejando errores
    df['Fecha Down'] = pd.to_datetime(df['Fecha Down'], errors='coerce')
    df['Fecha Up'] = pd.to_datetime(df['Fecha Up'], errors='coerce')
    # df['Agencia_base'] = df['Enlace'].apply(lambda x: ' '.join(x.split()[:-2]).lower().replace("backup", "").replace("principal", "").strip())
    df['Agencia_base'] = df['Enlace'].apply(lambda x: ' '.join(x.split()[:-1])  # Elimina el proveedor (última palabra)
                                         .replace("Principal", "")          # Elimina la palabra "Principal"
                                         .replace("Backup", "")             # Elimina la palabra "Backup"
                                         .strip())                          # Elimina espacios en blanco extras
    df['Proveedor'] = df['Enlace'].apply(lambda x: x.split()[-1])
    # Crear una copia del DataFrame para realizar ajustes
    data_adjusted = df.copy()
 
    # Agrupar por 'Agencia' o una parte común del nombre que no incluya 'Principal' o 'Backup'
   
 
    for agency, group in df.groupby('Agencia_base'):
        # Filtrar enlaces principales en estado 'reboot', ignorando mayúsculas/minúsculas
        principal_reboot = group[(group['Estado'] == 'Reboot') & (group['Enlace'].str.contains('Principal', case=False))]
        print(f"Este enlace es principal: \n", principal_reboot)
        # Si hay un principal en reboot
        if not principal_reboot.empty:
            for i, principal in principal_reboot.iterrows():
                # Buscar el backup correspondiente (misma agencia, mismo proveedor)
                backup = group[(group['Enlace'].str.contains('Backup', case=False)) ]
               
                print(f"Backups correspondientes:\n", backup)
                # Si existe el enlace backup
                if not backup.empty:
                    for j, backup_row in backup.iterrows():
                        # Verificar que las fechas coincidan entre el principal y el backup
                        fecha_down_principal = principal['Fecha Down']
                        fecha_up_principal = principal['Fecha Up']
                        fecha_down_backup = backup_row['Fecha Down']
                        fecha_up_backup = backup_row['Fecha Up']
                        print("fecha_down_principal:",fecha_down_principal)
                        print("fecha_down_backup:",fecha_down_backup)
                        # Asegurarse de que las fechas no sean NaT antes de comparar
                        if pd.notna(fecha_down_principal) and pd.notna(fecha_down_backup) and \
                            pd.notna(fecha_up_principal) and pd.notna(fecha_up_backup):
 
                            # Comparar las fechas de down y up entre el principal y el backup con un margen de tiempo
                            if (np.abs(fecha_down_principal - fecha_down_backup) <= time_margin) and \
                            (np.abs(fecha_up_principal - fecha_up_backup) <= time_margin):
 
                                # Si el estado del backup es 'Caido y Recuperado', cambiar a 'reboot'
                                if backup_row['Estado'] == 'Caído y recuperado':
                                    data_adjusted.at[j, 'Estado'] = 'Reboot'  # Cambiar el estado del backup a 'reboot'
                                    print(f"El estado del enlace backup '{backup_row['Enlace']}' se cambió a 'reboot'.")
        if group['Proveedor'].nunique() > 1:  # Más de un proveedor para la misma agencia
            #print("###Mas de un proveedor para la misma agencia")
            #print(group.head(20))
            for i, row in group.iterrows():
                # Buscar entradas similares dentro del grupo
                similar_entries = group[
                    (np.abs(group['Fecha Down'] - row['Fecha Down']) <= time_margin) &
                    (np.abs(group['Fecha Up'] - row['Fecha Up']) <= time_margin) &
                    (group['Proveedor'] != row['Proveedor'])
                ]
               
                # Si hay entradas similares y el estado actual no es 'reboot' pero otra entrada sí lo es
                if not similar_entries.empty:
                    if 'reboot' in similar_entries['Estado'].values and row['Estado'] != 'reboot':
                        data_adjusted.loc[i, 'Estado'] = 'Reboot'
    # Reiniciar el índice del DataFrame ajustado
    data_adjusted.reset_index(drop=True, inplace=True)
 
    return data_adjusted
                
# Imprimir ejemplo
df_final=corregir_estados_reboot(df_resultado)
df_final.to_excel('files/reporte_eventos_procesado.xlsx', index=False)

print("Archivo generado exitosamente: files/reporte_eventos_procesado.xlsx")