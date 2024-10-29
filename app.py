import streamlit as st
import pandas as pd
import re
from datetime import datetime

# Función para extraer timestamp de cada línea
def extract_timestamp(line):
    match = re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+\d{4}", line)
    if match:
        return datetime.strptime(match.group(), "%Y-%m-%dT%H:%M:%S.%f+0000")
    return None

# Función para analizar periodos de pérdida de comunicación
def analyze_communication_loss(file_content):
    disconnection_start = None
    connection_periods = []

    for line in file_content:
        if "Check connection. offline 1/" in line:
            disconnection_start = extract_timestamp(line)
        elif "Connect with server success" in line and disconnection_start:
            reconnection_time = extract_timestamp(line)
            if reconnection_time and disconnection_start:
                duration = reconnection_time - disconnection_start
                connection_periods.append({
                    "Start Time": disconnection_start,
                    "End Time": reconnection_time,
                    "Duration": duration
                })
            disconnection_start = None

    return pd.DataFrame(connection_periods)
# Función para extraer el valor de `chgRoutine` de una línea si está presente
def extract_chg_routine(line):
    match = re.search(r'chgRoutine\s*=\s*(\d+)', line)
    if match:
        return int(match.group(1))
    return None
# Función para extraer el valor de `DC_Status`
def extract_dc_status(line):
    match = re.search(r'DC_Status\s*=\s*(\d+)', line)
    if match:
        return int(match.group(1))
    return None

# Función para extraer el valor de `stopReason`
def extract_stop_reason(line):
    match = re.search(r'stopReason\s*=\s*(\d+)', line)
    if match:
        return int(match.group(1))
    return None
# Función para analizar comandos de recarga remota y sus respuestas
def analyze_remote_commands(file_content):
    remote_command_periods = []
    total_lines = len(file_content)

    for i, line in enumerate(file_content):
        if "RemoteStartTransaction" in line:
            match_command = re.search(r'RemoteStartTransaction",\{"idTag":"([^"]+)","connectorId":(\d+)\}\]', line)
            if match_command:
                transaction_id = match_command.group(1)
                connector_id = match_command.group(2)
                command_time = extract_timestamp(line)
                
                # Buscar respuesta de aceptación
                for j in range(i + 1, total_lines):
                    if '{"status":"Accepted"}' in file_content[j]:
                        response_time = extract_timestamp(file_content[j])
                        if response_time and command_time:
                            duration = response_time - command_time
                            remote_command_periods.append({
                                "Transaction ID": transaction_id,
                                "Connector ID": connector_id,
                                "Command Time": command_time,
                                "Response Time": response_time,
                                "Duration": duration
                            })
                        break

    return pd.DataFrame(remote_command_periods)

# Función para calcular la energía suministrada de cada transacción
def analyze_energy_supplied(file_content):
    transaction_pattern = r'"transactionId":\s*(\d+)'
    initial_energy_pattern = r'"context":\s*"Transaction\.Begin".*?"value":\s*"(\d+)"'
    final_energy_pattern = r'"meterStop":\s*(\d+)'
    transaction_start_end = {}

    for line in file_content:
        transaction_match = re.search(transaction_pattern, line)
        if transaction_match:
            transaction_id = transaction_match.group(1)

            initial_match = re.search(initial_energy_pattern, line)
            if initial_match:
                initial_energy = int(initial_match.group(1))
                if transaction_id not in transaction_start_end:
                    transaction_start_end[transaction_id] = {"initial": initial_energy, "final": None}

            final_match = re.search(final_energy_pattern, line)
            if final_match:
                final_energy = int(final_match.group(1))
                if transaction_id in transaction_start_end:
                    transaction_start_end[transaction_id]["final"] = final_energy
                else:
                    transaction_start_end[transaction_id] = {"initial": None, "final": final_energy}

    energy_summary = []
    for trans_id, data in transaction_start_end.items():
        if data["initial"] is not None and data["final"] is not None:
            energy_supplied = data["final"] - data["initial"]
            energy_summary.append({
                "Transaction ID": trans_id,
                "Initial Energy (Wh)": data["initial"],
                "Final Energy (Wh)": data["final"],
                "Energy Supplied (Wh)": energy_supplied
            })

    return pd.DataFrame(energy_summary)

# Función para extraer el valor de `Energy.Active.Import.Register` en `Transaction.Begin`
def extract_start_count(line):
    match = re.search(r'"value": "(\d+)", "context": "Transaction\.Begin", "measurand": "Energy.Active.Import.Register","location": "Outlet", "unit": "Wh"', line)
    if match:
        return int(match.group(1))
    return None

# Función para extraer el valor de `meterStop` en el último registro
def extract_stop_count(line):
    match = re.search(r'"meterStop": (\d+)', line)
    if match:
        return int(match.group(1))
    return None
# Función para extraer el valor de `DC_Status`
def extract_dc_status(line):
    match = re.search(r'DC_Status\s*=\s*(\d+)', line)
    if match:
        return int(match.group(1))
    return None

# Función para extraer el valor de `stopReason`
def extract_stop_reason(line):
    match = re.search(r'stopReason\s*=\s*(\d+)', line)
    if match:
        return int(match.group(1))
    return None
# Función para encontrar el inicio y fin de cada transacción basado en `transactionId` y capturar `chgRoutine`
def find_transaction_start_end(file_content):
    transaction_pattern = r'"transactionId": (\d+)'
    transaction_events = {}

    # Primer recorrido para registrar el inicio, fin, Start_count y Stop_count de cada transacción
    for line in file_content:
        transaction_match = re.search(transaction_pattern, line)
        if transaction_match:
            transaction_id = transaction_match.group(1)
            timestamp = extract_timestamp(line)

            # Extraer valores iniciales y finales
            start_count = extract_start_count(line)
            stop_count = extract_stop_count(line)

            if transaction_id in transaction_events:
                # Actualizar el tiempo de fin y el Stop_count si ya existe la transacción
                if timestamp:
                    transaction_events[transaction_id]["end_time"] = timestamp
                if stop_count is not None:
                    transaction_events[transaction_id]["stop_count"] = stop_count
            else:
                # Guardar inicio, fin, Start_count, Stop_count y listas para chgRoutine, DC_Status y stopReason
                transaction_events[transaction_id] = {
                    "start_time": timestamp,
                    "end_time": timestamp,
                    "start_count": start_count,
                    "stop_count": stop_count,
                    "chg_routines": [],
                    "dc_status_values": [],
                    "stop_reasons": []
                }

    # Segundo recorrido para buscar `chgRoutine`, `DC_Status`, y `stopReason` dentro del tiempo de cada transacción
    for line in file_content:
        timestamp = extract_timestamp(line)
        chg_routine = extract_chg_routine(line)
        dc_status = extract_dc_status(line)
        stop_reason = extract_stop_reason(line)

        if timestamp:
            for trans_id, data in transaction_events.items():
                # Verificar si el timestamp está en el rango de la transacción
                if data["start_time"] <= timestamp <= data["end_time"]:
                    if chg_routine is not None:
                        data["chg_routines"].append(chg_routine)
                    if dc_status is not None:
                        data["dc_status_values"].append(dc_status)
                    if stop_reason is not None:
                        data["stop_reasons"].append(stop_reason)

    # Crear un DataFrame con el resumen de las transacciones
    transaction_summary = []
    for trans_id, data in transaction_events.items():
        # Calcular la duración si las marcas de tiempo están disponibles
        duration = None
        if data["start_time"] and data["end_time"]:
            duration = data["end_time"] - data["start_time"]
        # Calcular la energía suministrada si los conteos están disponibles
        energy_wh = None
        if data["start_count"] is not None and data["stop_count"] is not None:
            energy_wh = data["stop_count"] - data["start_count"]
        # Determinar si se debe marcar como "Revisar"
        revisar = "Revisar" if duration and duration < pd.Timedelta(minutes=10) else "OK"

        transaction_summary.append({
            "Transaction ID": trans_id,
            "Start Time": data["start_time"],
            "End Time": data["end_time"],
            "Duration": duration,
            "Start_count": data["start_count"],
            "Stop_count": data["stop_count"],
            "Energy_Wh": energy_wh,
            "Revisar": revisar,
            "chgRoutine Values": data["chg_routines"],
            "DC_Status Values": data["dc_status_values"],
            "stopReason Values": data["stop_reasons"]
        })

    # Crear DataFrame
    df = pd.DataFrame(transaction_summary)
    return df


# Configuración de la página
st.title("Análisis de Logs de Sesiones de Carga (OCPP)")
st.write("Cargue un archivo de log para procesarlo y analizar la información de las sesiones de carga.")

# Subida del archivo de log
archivo_log = st.file_uploader("Sube tu archivo de log", type=["txt", "log"])

if archivo_log is not None:
    # Leer contenido del archivo
    contenido = archivo_log.read().decode('utf-8', errors='ignore')
    file_content = contenido.splitlines()

    # Análisis de pérdida de comunicación
    st.header("Análisis de Pérdida de Comunicación")
    communication_loss_df = analyze_communication_loss(file_content)
    st.dataframe(communication_loss_df)

    # Análisis de comandos de recarga remota
    st.header("Análisis de Comandos de Recarga Remota")
    remote_commands_df = analyze_remote_commands(file_content)
    st.dataframe(remote_commands_df)

    # Análisis de energía suministrada por transacción
    st.header("Análisis de Energía Suministrada por Transacción")
    energy_supplied_df = analyze_energy_supplied(file_content)
    st.dataframe(energy_supplied_df)

    # Análisis de transacciones con Start_count y Stop_count
    st.header("Análisis de Transacciones con Start_count y Stop_count")
    transaction_start_end_df = find_transaction_start_end(file_content)
    st.dataframe(transaction_start_end_df)

    # Opcional: Descarga de resultados
    st.subheader("Descargar Resultados")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        csv1 = communication_loss_df.to_csv(index=False).encode('utf-8')
        st.download_button("Descargar Pérdida de Comunicación", csv1, "communication_loss_periods.csv", "text/csv")
    with col2:
        csv2 = remote_commands_df.to_csv(index=False).encode('utf-8')
        st.download_button("Descargar Comandos Remotos", csv2, "remote_command_analysis.csv", "text/csv")
    with col3:
        csv3 = energy_supplied_df.to_csv(index=False).encode('utf-8')
        st.download_button("Descargar Energía Suministrada", csv3, "energy_supplied_per_transaction.csv", "text/csv")
    with col4:
        csv4 = transaction_start_end_df.to_csv(index=False).encode('utf-8')
        st.download_button("Descargar Transacciones", csv4, "transaction_start_end_analysis_with_energy.csv", "text/csv")
