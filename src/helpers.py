def save_file(data, file_path):
    with open(file_path, "w", encoding="utf-8") as output_file:
        output_file.write(str(data))