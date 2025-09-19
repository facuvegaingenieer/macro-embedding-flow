import pytest
from macro_embeddings_flow.main import MacroEtlEmbedding
def test_macro_etl(monkeypatch):
    # Patch extract para que no descargue realmente
    monkeypatch.setattr(
        "macro_embeddings_flow.extract.extract.extract_S3.extract",
        lambda url, dest: "datos_parquet/fake_chunk.parquet"
    )
    monkeypatch.setattr(
        "macro_embeddings_flow.transform.transform.transform_embedding.transform",
        lambda f, dest: ["datos_parquet/fake_chunk_transformed.parquet"]
    )
    monkeypatch.setattr(
        "macro_embeddings_flow.load.load.loadEmbedding.load",
        lambda chunks: ["datos_embedding/fake_embedding.parquet"]
    )
    
    urls3 = MacroEtlEmbedding("s3://mi-bucket/fake_chunk.parquet").run()
    assert urls3 == ["s3://mi-bucket/fake_chunk_transformed.parquet"]
