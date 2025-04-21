import pandas as pd
import numpy as np
from datetime import datetime
import re
from datetime import timedelta
# Constantes
ARCHIVO_ENTRADA = "files/Report_Testing_orion_ggcas.xlsx"
ARCHIVO_SALIDA = "files/Reporte_eventos.xlsx"
PROVEEDORES_VALIDOS = ['telconet', 'puntonet', 'cnt', 'movistar', 'cirion', 'claro', 'newaccess']

def cargar_datos(ruta_archivo):
    # Cargar el archivo y eliminar las dos primeras filas
    df = pd.read_excel(ruta_archivo, skiprows=2)

    # Ajustar la hora (restar 5 horas para zona horaria)
    df["EventTime"] = pd.to_datetime(df["EventTime"], errors='coerce') - timedelta(hours=5)

    return df
def extraer_proveedor(enlace):
    for prov in PROVEEDORES_VALIDOS:
        if prov.lower() in enlace.lower():
            return prov.capitalize()
    return None

def extraer_agencia_base(message):
    frases_clave = [
        'has stopped responding',
        'rebooted',
        'is responding again'
    ]
    mensaje_limpio = message
    for frase in frases_clave:
        if frase in mensaje_limpio.lower():
            idx = mensaje_limpio.lower().find(frase)
            mensaje_limpio = mensaje_limpio[:idx].strip()
            break
    return mensaje_limpio.strip()

def preprocesar_datos(df):
    # Filtrar solo filas con proveedor válido
    df["Proveedor"] = df["Message"].apply(extraer_proveedor)
    df = df[df["Proveedor"].notnull()]

    # Extraer nombre base del enlace
    df["Agencia_base"] = df["Message"].apply(extraer_agencia_base)

    return df

def construir_diccionario_reboots(df):
    """Crea un diccionario con fechas de reboot por agencia, redondeadas al minuto."""
    reboots = {}
    for _, fila in df[df['EventTypeName'].str.lower().str.contains('reboot')].iterrows():
        agencia = fila['Agencia_base']
        fecha = fila['EventTime'].replace(second=0, microsecond=0)
        reboots.setdefault(agencia, set()).add(fecha)
    return reboots

def hay_reboot_cercano(agencia, fecha_up, reboots_por_agencia):
    """Verifica si hay un reboot dentro de ±2 minutos del evento UP."""
    if agencia not in reboots_por_agencia or pd.isna(fecha_up):
        return False
    fecha_up_red = fecha_up.replace(second=0, microsecond=0)
    ventana = [fecha_up_red + timedelta(minutes=i) for i in range(-2, 3)]
    return any(f in reboots_por_agencia[agencia] for f in ventana)

def analizar_eventos(df):
    resultado = []
    reboots_por_agencia = construir_diccionario_reboots(df)

    for agencia, grupo in df.groupby('Agencia_base'):
        eventos = grupo.sort_values('EventTime')
        fecha_down = None
        fecha_up = None

        for _, fila in eventos.iterrows():
            evento = str(fila['EventTypeName']).lower()

            if 'down' in evento and fecha_down is None:
                fecha_down = fila['EventTime']
            elif 'up' in evento and fecha_down is not None:
                fecha_up = fila['EventTime']
                hay_reboot = hay_reboot_cercano(agencia, fecha_up, reboots_por_agencia)
                estado = 'Reboot' if hay_reboot else 'Caído y recuperado'
                tiempo = round((fecha_up - fecha_down).total_seconds() / 60)

                resultado.append({
                    'Enlace': agencia,
                    'Fecha Down': fecha_down,
                    'Fecha Up': fecha_up,
                    'Tiempo': tiempo,
                    'Estado': estado,
                    'Agencia_base': fila['Agencia_base'],
                    'Proveedor': fila['Proveedor']
                })

                fecha_down = None
                fecha_up = None

        # Si quedó una caída sin subida
        if fecha_down is not None and fecha_up is None:
            resultado.append({
                'Enlace': agencia,
                'Fecha Down': fecha_down,
                'Fecha Up': None,
                'Tiempo': None,
                'Estado': 'Caído',
                'Agencia_base': grupo.iloc[-1]['Agencia_base'],
                'Proveedor': grupo.iloc[-1]['Proveedor']
            })

    return pd.DataFrame(resultado)
def corregir_estados_reboot(df, time_margin='2min'):
    # Asegurar que las fechas sean tipo datetime
    df['Fecha Down'] = pd.to_datetime(df['Fecha Down'], errors='coerce')
    df['Fecha Up'] = pd.to_datetime(df['Fecha Up'], errors='coerce')

    # Extraer base de la agencia y proveedor desde el campo 'Enlace'
    df['Agencia_base'] = df['Enlace'].apply(lambda x: ' '.join(x.split()[:-1])
                                            .replace("Principal", "")
                                            .replace("Backup", "")
                                            .strip())
    df['Proveedor'] = df['Enlace'].apply(lambda x: x.split()[-1])

    time_margin = pd.Timedelta(time_margin)
    ajustado = df.copy()

    for agencia, grupo in df.groupby('Agencia_base'):
        # Buscar enlaces principales en Reboot
        principales = grupo[(grupo['Estado'] == 'Reboot') & (grupo['Enlace'].str.contains('Principal', case=False))]

        for _, principal in principales.iterrows():
            fecha_down_p = principal['Fecha Down']
            fecha_up_p = principal['Fecha Up']

            # Buscar backups del mismo grupo
            backups = grupo[grupo['Enlace'].str.contains('Backup', case=False)]

            for idx, backup in backups.iterrows():
                fecha_down_b = backup['Fecha Down']
                fecha_up_b = backup['Fecha Up']

                if pd.notna(fecha_down_p) and pd.notna(fecha_down_b) and \
                   pd.notna(fecha_up_p) and pd.notna(fecha_up_b):

                    down_match = abs(fecha_down_p - fecha_down_b) <= time_margin
                    up_match = abs(fecha_up_p - fecha_up_b) <= time_margin

                    if down_match and up_match and backup['Estado'] == 'Caído y recuperado':
                        ajustado.at[idx, 'Estado'] = 'Reboot'

        # Comparación entre diferentes proveedores
        if grupo['Proveedor'].nunique() > 1:
            for i, fila in grupo.iterrows():
                similares = grupo[
                    (abs(grupo['Fecha Down'] - fila['Fecha Down']) <= time_margin) &
                    (abs(grupo['Fecha Up'] - fila['Fecha Up']) <= time_margin) &
                    (grupo['Proveedor'] != fila['Proveedor'])
                ]

                if not similares.empty and 'Reboot' in similares['Estado'].values and fila['Estado'] != 'Reboot':
                    ajustado.at[i, 'Estado'] = 'Reboot'

    ajustado.reset_index(drop=True, inplace=True)
    return ajustado
# ---------------------- MAIN ----------------------

def main():
    df = cargar_datos(ARCHIVO_ENTRADA)
    df_limpio = preprocesar_datos(df)
    df_eventos = analizar_eventos(df_limpio)
    df_final = corregir_estados_reboot(df_eventos)
    df_final.to_excel(ARCHIVO_SALIDA, index=False)
    print(f"✅ Archivo generado exitosamente: {ARCHIVO_SALIDA}")

if __name__ == "__main__":
    main()