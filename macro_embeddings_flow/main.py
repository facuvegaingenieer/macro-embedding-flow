from macro_embeddings_flow.extract.extract import extract_S3
from macro_embeddings_flow.transform.transform import transform_embedding
from macro_embeddings_flow.load.load import loadEmbedding
import logging
from dotenv import load_dotenv


logging.basicConfig(
    level=logging.INFO,              # Nivel mínimo de logs a mostrar
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='embedding.log',             # Opcional: guarda los logs en un archivo
    filemode='a'                    # 'a' append, 'w' overwrite
)


def MacroEtlEmbedding(url: str) -> str | None:
    """
    se le pasa la url de un s3 con chunks, se descarga el archivo se transforma a embedding de 768 dimensiones y
    y los vuelve a subir ya vectorizados

    You pass the URL of an S3 file with chunks, download the file, transform it into a 768-dimensional embedding, and upload it again, already vectorized.
    """

    # 1️⃣ Extraer
    archivo = extract_S3.extract(url)
    if not archivo:
        logging.error(f"Error al descargar los chunks desde {url}")
        return None

    # 2️⃣ Transformar
    embedding_data = transform_embedding.transform(archivo)
    if not embedding_data :
        logging.error(f"Error al transformar los chunks desde {url}")
        return None

    # 3️⃣ Cargar a S3
    s3_embedding = loadEmbedding.load(embedding_data )
    if not s3_embedding:
        logging.error(f"Error al subir chunks a S3 desde {url}")
        return None

    # 4️⃣ Retornar URLs de S3
    return s3_embedding