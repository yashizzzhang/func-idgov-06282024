import os
import json

def load_settings(filepath):
    with open(filepath, 'r') as file:
        settings = json.load(file)
    for key, value in settings.items():
        os.environ[key] = str(value)  # Ensure all values are strings
        print(f"Set {key} to {value}")  # Print for verification

if __name__ == "__main__":
    load_settings('local.settings.json')
