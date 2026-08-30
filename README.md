# **DataTrek**

DataTrek is a data pipeline project focused on transforming and managing location data. It uses Apache Spark for processing, Apache Iceberg for organizing and storing data tables, Project Nessie for version control, and Apache Airflow for orchestrating the transformation workflows.


**Data sources**:
- **News Data**: Sourced from [**Newscatcher API**](https://www.newscatcherapi.com/) for real-time news insights

## Uploading images

Set the local directories in your environment (or `.env`):

```bash
export PATH_TO_BOOK_IMAGES='/Users/dejiemiabata/Documents/book-images'
export PATH_TO_CHESS_GIFS='/Users/dejiemiabata/Documents/chess-gifs'
export PATH_TO_TRAVEL_IMAGES='/Users/dejiemiabata/Documents/travel-images'
```

Run the upload script with the target you want:

```bash
python ingestion/upload_raw_location_data.py book-images
python ingestion/upload_raw_location_data.py chess-gifs
python ingestion/upload_raw_location_data.py travel-images
# Optional folder prefix for a new destination, for example:
python ingestion/upload_raw_location_data.py travel-images --prefix yosemite
```

Travel images are uploaded with their relative local path, so a file at
`travel-images/guatemala/example.JPG` becomes `travel-2/guatemala/example.JPG`.
