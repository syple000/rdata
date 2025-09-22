import os
import fnmatch

def clean_files(root_dir=None):
    if root_dir is None:
        root_dir = os.path.dirname(os.path.abspath(__file__))

    patterns = ["*.json"]

    for dirpath, _, filenames in os.walk(root_dir):
        if 'target' in dirpath:
            continue
        for pattern in patterns:
            for name in fnmatch.filter(filenames, pattern):
                full_path = os.path.join(dirpath, name)
                try:
                    os.remove(full_path)
                    print(f"Deleted: {full_path}")
                except OSError as e:
                    print(f"Failed to delete {full_path}: {e}")

if __name__ == "__main__":
    clean_files()