import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re

# Constantes
ARCHIVO_ENTRADA = "files/Report_Testing_orion_ggcas.xlsx"
ARCHIVO_SALIDA = "files/Reporte_Incidentes_Enlaces.xlsx"
PROVEEDORES_VALIDOS = ['telconet', 'puntonet', 'cnt', 'movistar', 'cirion', 'claro', 'newaccess']

def cargar_datos(ruta_archivo):
    df = pd.read_excel(ruta_archivo, skiprows=2)
    df["EventTime"] = pd.to_datetime(df["EventTime"], errors='coerce') - timedelta(hours=5)
    df["EventTime"] = df["EventTime"].dt.floor('min')
    return df

def extraer_proveedor(enlace):
    for prov in PROVEEDORES_VALIDOS:
        if prov.lower() in enlace.lower():
            return prov.capitalize()
    return None

def extraer_agencia_base(message):
    frases_clave = ['has stopped responding', 'rebooted', 'is responding again']
    mensaje_limpio = message
    for frase in frases_clave:
        if frase in mensaje_limpio.lower():
            idx = mensaje_limpio.lower().find(frase)
            mensaje_limpio = mensaje_limpio[:idx].strip()
            break
    return mensaje_limpio.strip()

def preprocesar_datos(df):
    df["Proveedor"] = df["Message"].apply(extraer_proveedor)
    df = df[df["Proveedor"].notnull()]
    df["Agencia_base"] = df["Message"].apply(extraer_agencia_base)
    return df

def construir_diccionario_reboots(df):
    reboots = {}
    for _, fila in df[df['EventTypeName'].str.lower().str.contains('reboot')].iterrows():
        agencia = fila['Agencia_base']
        fecha = fila['EventTime'].replace(second=0, microsecond=0)
        reboots.setdefault(agencia, set()).add(fecha)
    return reboots

def hay_reboot_cercano(agencia, fecha_up, reboots_por_agencia):
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
    df['Fecha Down'] = pd.to_datetime(df['Fecha Down'], errors='coerce')
    df['Fecha Up'] = pd.to_datetime(df['Fecha Up'], errors='coerce')

    df['Agencia_base'] = df['Enlace'].apply(lambda x: ' '.join(x.split()[:-1])
                                            .replace("Principal", "")
                                            .replace("Backup", "")
                                            .strip())
    df['Proveedor'] = df['Enlace'].apply(lambda x: x.split()[-1])
    time_margin = pd.Timedelta(time_margin)
    ajustado = df.copy()

    for agencia, grupo in df.groupby('Agencia_base'):
        principales = grupo[(grupo['Estado'] == 'Reboot') & (grupo['Enlace'].str.contains('Principal', case=False))]

        for _, principal in principales.iterrows():
            fecha_down_p = principal['Fecha Down']
            fecha_up_p = principal['Fecha Up']
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

def pedir_rango_fechas():
    inicio_str = input("📅 Ingresa la fecha de inicio (dd/mm/yyyy): ")
    fin_str = input("📅 Ingresa la fecha de fin (dd/mm/yyyy): ")
    inicio = datetime.strptime(inicio_str, "%d/%m/%Y")
    fin = datetime.strptime(fin_str, "%d/%m/%Y")
    return inicio, fin

def generar_hojas_madrugada(df, escritor, fecha_inicio_usuario, fecha_fin_usuario):
    # Convertimos a datetime sin hora para comparar solo la parte de fecha
    fecha_inicio_usuario = fecha_inicio_usuario.replace(hour=0, minute=0, second=0, microsecond=0)
    fecha_fin_usuario = fecha_fin_usuario.replace(hour=0, minute=0, second=0, microsecond=0)

    # Filtrar solo registros con Fecha Down dentro del rango solicitado
    fechas_validas = df[
        (df['Fecha Down'].notnull()) &
        (df['Fecha Down'].dt.date >= fecha_inicio_usuario.date()) &
        (df['Fecha Down'].dt.date <= fecha_fin_usuario.date())
    ]['Fecha Down'].dt.date.unique()

    fechas_validas = sorted(fechas_validas)

    for fecha in fechas_validas:
        inicio_madrugada = datetime.combine(fecha, datetime.min.time()) + timedelta(hours=20)
        fin_madrugada = inicio_madrugada + timedelta(hours=12)
        nombre_hoja = f"{fecha.strftime('%d')}-{(fecha + timedelta(days=1)).strftime('%d')}_Madrugada"

        registros = df[
            ((df['Fecha Down'] >= inicio_madrugada) & (df['Fecha Down'] < fin_madrugada)) |
            ((df['Fecha Up'] >= inicio_madrugada) & (df['Fecha Up'] < fin_madrugada))
        ]

        if not registros.empty:
            registros.to_excel(escritor, sheet_name=nombre_hoja[:31], index=False)


def generar_hojas_dia(df, escritor, fecha_inicio, fecha_fin):
    """
    Crea hojas en el Excel con registros del día (08h00 a 20h00) por cada fecha entre fecha_inicio y fecha_fin.
    """
    fecha_actual = fecha_inicio

    while fecha_actual <= fecha_fin:
        inicio_dia = datetime.combine(fecha_actual, datetime.min.time()) + timedelta(hours=8)
        fin_dia = datetime.combine(fecha_actual, datetime.min.time()) + timedelta(hours=20)

        registros = df[
            (df['Fecha Down'] >= inicio_dia) & (df['Fecha Down'] < fin_dia)
        ]

        if not registros.empty:
            nombre_hoja = f"{fecha_actual.day:02d}_dia"
            registros.to_excel(escritor, sheet_name=nombre_hoja[:31], index=False)

        fecha_actual += timedelta(days=1)

def rango_reporte_madrugada(df_corregido):
    # 🗓️ Pedir fechas al usuario
    fecha_inicio_str = input("📅 Ingresa la fecha de inicio (dd/mm/yyyy): ")
    fecha_fin_str = input("📅 Ingresa la fecha de fin (dd/mm/yyyy): ")

    fecha_inicio = datetime.strptime(fecha_inicio_str, "%d/%m/%Y")
    fecha_fin = datetime.strptime(fecha_fin_str, "%d/%m/%Y")

    with pd.ExcelWriter(ARCHIVO_SALIDA, engine='xlsxwriter') as writer:
        df_corregido.to_excel(writer, sheet_name='Incidentes Total', index=False)
        generar_hojas_madrugada(df_corregido, writer, fecha_inicio, fecha_fin)

    print(f"✅ Archivo generado exitosamente: {ARCHIVO_SALIDA}")
def rango_reporte_dia(df_corregido):
    # 🗓️ Pedir fechas al usuario
    fecha_inicio_str = input("📅 Ingresa la fecha de inicio (dd/mm/yyyy): ")
    fecha_fin_str = input("📅 Ingresa la fecha de fin (dd/mm/yyyy): ")

    fecha_inicio = datetime.strptime(fecha_inicio_str, "%d/%m/%Y")
    fecha_fin = datetime.strptime(fecha_fin_str, "%d/%m/%Y")

    with pd.ExcelWriter(ARCHIVO_SALIDA, engine='xlsxwriter') as writer:
        df_corregido.to_excel(writer, sheet_name='Incidentes Total', index=False)
        generar_hojas_dia(df_corregido, writer, fecha_inicio, fecha_fin)

    print(f"✅ Archivo generado exitosamente: {ARCHIVO_SALIDA}")
# ---------------------- MAIN ----------------------
def procesando_datos():
    df = cargar_datos(ARCHIVO_ENTRADA)
    df_limpio = preprocesar_datos(df)
    df_eventos = analizar_eventos(df_limpio)
    df_corregido = corregir_estados_reboot(df_eventos)
    print(":::: Se ha procesado los datos ::::")
    return df_corregido
def main():
    flag=True
    while flag:
        print("1.- Reporte madrugada")
        print("2.- Reporte del dìa")
        print("3.- Salir")
        opcion=input("Seleccionar una opcion: ")
        if opcion=="1":
            rango_reporte_madrugada(procesando_datos())
        elif opcion=="2":
            rango_reporte_dia(procesando_datos())
        elif opcion=="3":
            flag=False
    
if __name__ == "__main__":
    main()