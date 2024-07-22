import os
import hashlib
import pytest

from news_extractor_engine.utils.checksum_validator import (
    generate_checksum,
    check_or_create_checksum,
)

DATA_FILE_PATH = "tests/utils/sample/sample.txt"
CHECKSUM_FILE_PATH = "tests/utils/sample/valid.sha2"
FAKE_CHECKSUM_FILE_PATH = "tests/utils/sample/fake.sha2"


@pytest.mark.checksum_generation
def test_if_generates_checksum():
    """Verify that the generate_checksum function generates checksum for file."""
    checksum = generate_checksum(DATA_FILE_PATH)
    assert isinstance(checksum, str)
    assert len(checksum) == 64


@pytest.mark.checksum_file_generation
def test_checksum_file_generation():
    """Verify that the check_or_create_checksum function generates checksum file and file contains relevant checksum."""
    if os.path.exists(CHECKSUM_FILE_PATH):
        os.remove(CHECKSUM_FILE_PATH)
    check_or_create_checksum(DATA_FILE_PATH, CHECKSUM_FILE_PATH)
    checksum = generate_checksum(DATA_FILE_PATH)
    assert os.path.exists(CHECKSUM_FILE_PATH)
    with open(CHECKSUM_FILE_PATH, "r") as checksum_file:
        stored_checksum = checksum_file.read().strip()
        assert stored_checksum == checksum


@pytest.mark.checksum_file_generation
def test_correct_checksum_file_acceptance():
    """Verify that the check_or_create_checksum function returns True if checksum file is correct."""
    assert check_or_create_checksum(DATA_FILE_PATH, CHECKSUM_FILE_PATH) == True


@pytest.mark.checksum_file_generation
def test_incorrect_checksum_file_renewal():
    """Verify that the check_or_create_checksum function returns False if checksum file is incorrect and renews the checksum file."""
    if os.path.exists(FAKE_CHECKSUM_FILE_PATH):
        with open(FAKE_CHECKSUM_FILE_PATH, "w") as checksum_file:
            checksum_file.write(
                hashlib.sha256("fake_checksum".encode("utf-8")).hexdigest()
            )
    assert check_or_create_checksum(DATA_FILE_PATH, FAKE_CHECKSUM_FILE_PATH) == False
    assert os.path.exists(FAKE_CHECKSUM_FILE_PATH)
    checksum = generate_checksum(DATA_FILE_PATH)
    with open(FAKE_CHECKSUM_FILE_PATH, "r") as checksum_file:
        stored_checksum = checksum_file.read().strip()
        assert stored_checksum == checksum
