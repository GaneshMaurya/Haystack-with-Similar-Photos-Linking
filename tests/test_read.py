import pytest, httpx

@pytest.mark.asyncio
async def test_read_photo(api_service):
    upload_url = "http://localhost:8080/upload"
    read_url = "http://localhost:8080/photo"

    # Use a single client session for both upload and read
    async with httpx.AsyncClient() as client:
        # 1. Upload
        r = await client.post(
            upload_url,
            files={"file": ("t.jpg", b"testphoto", "image/jpeg")}
        )
        photo_id = r.json()["photo_id"]

        # 2. Read
        r2 = await client.get(f"{read_url}/{photo_id}")

    assert r2.status_code == 200
    assert r2.content == b"testphoto"