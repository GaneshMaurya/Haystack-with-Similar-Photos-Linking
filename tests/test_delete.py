import pytest, httpx

@pytest.mark.asyncio
async def test_delete_photo(api_service):
    upload_url = "http://localhost:8080/upload"
    delete_url = "http://localhost:8080/photo"
    read_url = "http://localhost:8080/photo"

    img = b"delete-me"

    # Use a single client session for all three steps
    async with httpx.AsyncClient() as client:
        # 1. Upload
        up = await client.post(
            upload_url,
            files={"file": ("x.jpg", img, "image/jpeg")}
        )
        pid = up.json()["photo_id"]

        # 2. Delete
        delr = await client.delete(f"{delete_url}/{pid}")
        assert delr.status_code == 200

        # 3. Verify Read Fails
        r2 = await client.get(f"{read_url}/{pid}")

    assert r2.status_code == 410