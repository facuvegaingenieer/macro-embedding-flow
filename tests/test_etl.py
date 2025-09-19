import pytest
from macro_embeddings_flow.main import MacroEtlEmbedding
def test_macro_etl(monkeypatch):
    # Patch extract para que no descargue realmente
    monkeypatch.setattr(
        "macro_embeddings_flow.extract.extract.extract_S3.extract",
        lambda url, dest: "datos_parquet/fake_chunk.parquet"  # acepta url y dest
    )
    monkeypatch.setattr(
        "macro_embeddings_flow.transform.transform.transform_embedding.transform",
        lambda parquet_path, dest: ["datos_parquet/fake_chunk_transformed.parquet"]  # acepta parquet_path y dest
    )
    monkeypatch.setattr(
        "macro_embeddings_flow.load.load.loadEmbedding.load",
        lambda chunks: ["s3://mi-bucket/fake_chunk_transformed.parquet"]  # devuelve URL S3 final
    )
    
    # Ejecutar pipeline completo
    urls3 = MacroEtlEmbedding("s3://mi-bucket/fake_chunk.parquet").run()
    
    # Verificar que la salida es la URL del embedding subido a S3
    assert urls3 == ["s3://mi-bucket/fake_chunk_transformed.parquet"]

