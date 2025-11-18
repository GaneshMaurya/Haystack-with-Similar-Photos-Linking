import pytest, subprocess, time, os, signal
import httpx

def start_service(cmd, cwd=None):
    full_cmd = f"python -m uvicorn {cmd}"
    proc = subprocess.Popen(full_cmd, shell=True, cwd=cwd)
    time.sleep(1.0)
    return proc

def stop_service(proc):
    proc.send_signal(signal.SIGINT)
    proc.terminate()


@pytest.fixture(scope="session")
def directory_service():
    proc = start_service(
        "main:app --port 8001 --reload", 
        cwd="services/directory" 
    )
    yield
    stop_service(proc)


@pytest.fixture(scope="session")
def store1():
    os.environ["STORE_ID"] = "store1"
    os.environ["VOLUME_PATH"] = "./testdata/store1/volume_v1.dat"
    os.makedirs("./testdata/store1", exist_ok=True)

    proc = start_service(
        "main:app --port 8101 --reload", 
        cwd="services/store"
    )
    yield
    stop_service(proc)


@pytest.fixture(scope="session")
def store2():
    os.environ["STORE_ID"] = "store2"
    os.environ["VOLUME_PATH"] = "./testdata/store2/volume_v1.dat"
    os.makedirs("./testdata/store2", exist_ok=True)

    proc = start_service(
        "main:app --port 8102 --reload", 
        cwd="services/store"
    )
    yield
    stop_service(proc)


@pytest.fixture(scope="session")
def api_service(directory_service, store1, store2):
    os.environ["DIRECTORY_URL"] = "http://localhost:8001"
    os.environ["RABBITMQ_URL"] = "dummy"

    proc = start_service(
        "main:app --port 8080 --reload",
        cwd="services/api"
    )
    yield
    stop_service(proc)
