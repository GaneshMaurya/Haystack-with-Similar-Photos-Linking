import pytest, httpx

@pytest.mark.asyncio
async def test_upload(api_service):
    url = "http://localhost:8080/upload"

    image_bytes = b"fake-jpeg-data"

    async with httpx.AsyncClient() as client:
        files = {"file": ("test.jpg", image_bytes, "image/jpeg")}
        r = await client.post(url, files=files)

    assert r.status_code == 200
    data = r.json()

    assert "photo_id" in data
    assert "logical_volume" in data
    assert "cookie" in data