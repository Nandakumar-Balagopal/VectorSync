import pytest
from app.models.requests import EmbeddingRecord, BatchEmbedRequest, QueryEmbedRequest


def test_embedding_record_text_validator():
    # Should raise for empty or whitespace text
    with pytest.raises(ValueError):
        EmbeddingRecord(
            vector_id="v1",
            source_table="t",
            source_row_id="r1",
            text="   ",
            model_name="m",
            provider="self_hosted",
        )
    # Should not raise for valid text
    rec = EmbeddingRecord(
        vector_id="v2",
        source_table="t",
        source_row_id="r2",
        text="valid",
        model_name="m",
        provider="self_hosted",
    )
    assert rec.text == "valid"

def test_batch_embed_request_records_validator():
    # Should raise for empty records
    with pytest.raises(ValueError):
        BatchEmbedRequest(request_id="r", records=[])
    # Should not raise for non-empty records
    req = BatchEmbedRequest(
        request_id="r",
        records=[
            EmbeddingRecord(
                vector_id="v1",
                source_table="t",
                source_row_id="r1",
                text="x",
                model_name="m",
                provider="self_hosted",
            )
        ],
    )
    assert req.request_id == "r"

def test_query_embed_request_text_validator():
    # Should raise for empty text
    with pytest.raises(ValueError):
        QueryEmbedRequest(text="", model_name="m", provider="self_hosted")
    # Should not raise for valid text
    req = QueryEmbedRequest(text="abc", model_name="m", provider="self_hosted")
    assert req.text == "abc"
