import os

# Name of the output file
OUTPUT_FILE = "full_project_code.txt"

# Folders to ignore
IGNORE_DIRS = {
    ".git", "__pycache__", "venv", "env", ".idea", ".vscode", 
    "data", "volumes", "similarity_images"
}

# File extensions to ignore (binaries, images, databases)
IGNORE_EXTS = {
    ".pyc", ".jpg", ".jpeg", ".png", ".gif", 
    ".db", ".sqlite", ".dat", ".exe", ".o", ".a", 
    ".zip", ".tar", ".gz", ".7z"
}

def pack_project():
    root_dir = os.getcwd()
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as out_f:
        out_f.write(f"PROJECT EXPORT\n")
        out_f.write(f"Root: {root_dir}\n")
        out_f.write("="*80 + "\n\n")

        for dirpath, dirnames, filenames in os.walk(root_dir):
            # Modify dirnames in-place to skip ignored directories
            dirnames[:] = [d for d in dirnames if d not in IGNORE_DIRS]

            for filename in filenames:
                # Skip the output file itself and the script itself
                if filename in [OUTPUT_FILE, "export_code.py"]:
                    continue
                
                # Skip ignored extensions
                ext = os.path.splitext(filename)[1].lower()
                if ext in IGNORE_EXTS:
                    continue

                file_path = os.path.join(dirpath, filename)
                rel_path = os.path.relpath(file_path, root_dir)

                try:
                    with open(file_path, "r", encoding="utf-8") as in_f:
                        content = in_f.read()
                        
                    out_f.write(f"FILE PATH: {rel_path}\n")
                    out_f.write("-" * 80 + "\n")
                    out_f.write(content)
                    out_f.write("\n" + "=" * 80 + "\n\n")
                    print(f"Exported: {rel_path}")
                    
                except Exception as e:
                    print(f"Skipped (read error): {rel_path} - {e}")

    print(f"\nDone! All code saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    pack_project()