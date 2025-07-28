def get_data_from_blob_storage(blob_name, connection_string):
    """
    Liest eine Datei aus Azure Blob Storage und gibt den Inhalt als DataFrame zurück.

    Args:
        blob_name (str): Name der Datei im Blob Storage.

    Returns:
        pd.DataFrame: Inhalt der Datei als DataFrame.
    """
    # Establish connection to the Azure Blob Storage account
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Access the container
    container_name = "source-data"
    container_client = blob_service_client.get_container_client(container_name)

    # Download the file
    blob_client = container_client.get_blob_client(blob_name)

    # Use a temporary file to download the blob
    temp_file_path = blob_name 
    with open(temp_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    # Load the Parquet file into a DataFrame
    df = pd.read_parquet(temp_file_path)

    # Remove the temporary file if needed
    os.remove(temp_file_path)

    return df


def prepare_data(df):
    df.rename(columns={"datum":"datetime","Überschuss":"feed_in:kWh", "Produktion": "production:kWh", "Eigenverbrauch": "self-consumption:kWh",
                        "PLZ":"zip_code", "Ort":"city", "PanelPeakLeistung": "panel_peak_power:kWp", "Ausrichtung": "orientation", "Anstellwinkel": "tilt:deg",
                        "Ausrichtung_Grad": "orientation:deg",  'Installierte, nominale Speicherkapazität (kWh)': "battery_capacity:kWh",
                        "Kategorie":"category"},inplace=True)
    return df

