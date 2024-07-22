import hashlib
import os


def generate_checksum(file_path):
    """Generate a SHA-256 checksum based on file size and modified timestamp."""
    file_size = os.path.getsize(file_path)
    modified_time = os.path.getmtime(file_path)
    checksum_input = f"{file_size}{modified_time}".encode("utf-8")
    return hashlib.sha256(checksum_input).hexdigest()


def check_or_create_checksum(data_file_path, checksum_file_path):
    """Check the checksum of the data file against the checksum file, create if not exists."""
    if not os.path.exists(data_file_path):
        raise FileNotFoundError(f"The data file {data_file_path} does not exist.")

    generated_checksum = generate_checksum(data_file_path)

    is_valid_checksum = False
    if os.path.exists(checksum_file_path):
        with open(checksum_file_path, "r") as checksum_file:
            stored_checksum = checksum_file.read().strip()
            if stored_checksum == generated_checksum:
                is_valid_checksum = True
                return True
    if not is_valid_checksum:
        with open(checksum_file_path, "w") as checksum_file:
            checksum_file.write(generated_checksum)
        return False
